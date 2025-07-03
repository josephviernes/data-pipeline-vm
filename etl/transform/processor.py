#!/usr/bin/env python
# coding: utf-8

import os
from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.functions import regexp_replace, regexp_extract, col, when, to_timestamp


def main():
    bucket = os.environ.get("bucket")
    folder = os.environ.get("folder")
    gcs_connector_path = "/app/gcs-connector-hadoop3-latest.jar"
    spark_session = create_spark_session(gcs_connector_path)
    file_path = latest_file_path(bucket, folder)

    df = read_gcs_file(spark_session, bucket, file_path)

    clean_df = transform(df)

    to_bq(clean_df)


def latest_file_path(bucket, folder):
    # FILTER THE GCS BUCKET FOLDER FOR THE LATEST EARTHQUAKE DATA FILE NAME
    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)

    # looks for the latest file inside the bucket, extract the full path of the file inside the gcs bucket
    blobs = list(bucket.list_blobs(prefix=folder))    # List blobs with the given prefix (acts like folder)
    files = [blob for blob in blobs if not blob.name.endswith('/')]    # Filter only "files" (exclude anything ending with '/')
    latest_blob = max(files, key=lambda b: b.updated, default=None)
    file_path = latest_blob.name if latest_blob else None

    return file_path


def create_spark_session(gcs_connector_path):   
    # initiate a spark session, configure connection between spark and big query
    return SparkSession.builder \
        .master("local[*]") \
        .appName('Read From GCS Bucket') \
        .config("spark.jars", gcs_connector_path) \
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0') \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate() 


def read_gcs_file(spark_session, bucket, file_path_inside_bucket):
    # Create a spark dataframe
    # multiline=True negates the unneccesary creation of new line from /n every row
    return spark_session.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("multiline", True) \
        .csv(f"gs://{bucket}/{file_path_inside_bucket}")


def transform(df):
    # DATA TRANSFORMATIONS
    # Mapping manually every months to fix abbreviation
    months = {
        "Jan": "January", "Feb": "February", "Mar": "March", "Apr": "April",
        "May": "May", "Jun": "June", "Jul": "July", "Aug": "August",
        "Sep": "September", "Oct": "October", "Nov": "November", "Dec": "December"
    }
    # change Jan to January and so on
    for short, full in months.items():
        df = df.withColumn(
            "datetime",
            regexp_replace("datetime", rf"\b{short}\b", full)
        )
        
    # convert date_time values to timestamp type
    df = df.withColumn(
        "datetime", 
        to_timestamp("datetime", "dd MMMM yyyy - hh:mm a")
    )

    # convert latitude and longitude to decimal data type with specific number of digits
    df = df.withColumn(
        "latitude",
        col("latitude").cast("decimal(5,2)")
    ) \
        .withColumn(
        "longitude",
        col("longitude").cast("decimal(5,2)")
    )

    # convert depth column from string to integer type, set invalid entries into None/Null
    df = df.withColumn(
        "depth_km",
        when(col("depth_km").rlike(r"^\d+$"), col("depth_km").cast("int")).otherwise(None)
    )

    # convert magnitude to decimal date type
    df = df.withColumn(
        "magnitude",
        col("magnitude").cast("decimal(3,1)")
    )

    # delete nuisance whitespaces in relative_location
    df = df.withColumn(
        "relative_location",
        regexp_replace(df["relative_location"], "[\r\n\t]", ""))

    # replace multiple consecutive spaces with single space
    df = df.withColumn(
        "relative_location",
        regexp_replace(df["relative_location"], " +", " "))

    # create a new column 'province' from relative_location
    df = df.withColumn("province", regexp_extract(df["relative_location"], r"\((.*?)\)", 1))

    # band-aid fix to wrong capture of province
    df = df.withColumn(
        "province", 
        regexp_replace(df["province"], "Municipality Of Sarangani", "Davao Occidental"))
    return df

def to_bq(df):
    # EXPORTING TO BIG QUERY
    # save the dataframe as a table in big query, it uses a bucket to stage the file before creating the "temp_table" in big query
    # this also create or replace existing temp_table
    df.write \
        .format("bigquery") \
        .mode("overwrite") \
        .option("table", "finalproject-456408.earthquake_dataset.temp_table") \
        .option("temporaryGcsBucket", "phivolcs_earthquake_data") \
        .option("parentProject", "finalproject-456408") \
        .save()


if __name__ == "__main__":
    main()

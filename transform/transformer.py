#!/usr/bin/env python
# coding: utf-8


import os
import pyspark
from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.functions import regexp_replace, regexp_extract, col, when, to_timestamp


# filter the gcs bucket folder for the latest earthquake data file

# Initialize GCS client
storage_client = storage.Client.from_service_account_json("/home/joseph/Documents/dez_final_project/gsa/finalproject-456408-a18af71e91f6.json")
bucket = storage_client.bucket("phivolcs_earthquake_data")

# looks for the latest file inside the bucket, extract the full path of the file inside the gcs bucket
blobs = list(bucket.list_blobs(prefix="dailies/"))    # List blobs with the given prefix (acts like folder)
files = [blob for blob in blobs if not blob.name.endswith('/')]    # Filter only "files" (exclude anything ending with '/')
latest_blob = max(files, key=lambda b: b.updated, default=None)
file_name = latest_blob.name if latest_blob else NONE
print(file_name)

# initiate a spark session, configure connection between spark and gcs
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('earthquake') \
    .config("spark.jars", "/home/joseph/Documents/dez_final_project/python scraper/jars/gcs-connector-hadoop3-latest.jar") \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0') \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/joseph/Documents/dez_final_project/gsa/finalproject-456408-a18af71e91f6.json") \
    .getOrCreate() 

# Create a spark dataframe
# multiline=True negates the unneccesary creation of new line from /n every row
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiline", True) \
    .csv(f"gs://phivolcs_earthquake_data/{file_name}")




# DATA TRANSFORMATIONS

# Mapping manually every months to fix abbreviation
months = {
    "Jan": "January", "Feb": "February", "Mar": "March", "Apr": "April",
    "May": "May", "Jun": "June", "Jul": "July", "Aug": "August",
    "Sep": "September", "Oct": "October", "Nov": "November", "Dec": "December"
}

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

df = df.withColumn(
    "province", 
    regexp_replace(df["province"], "Municipality Of Sarangani", "Davao Occidental"))




# EXPORTING TO GOOGLE CLOUD

# renames the file accordingly
file_name2 = file_name.split("/")[-1].replace(".csv", "")
# saves the dataframe as csv in a separate folder called temp_data in the bucket
"""df.write \
    .option("header", True) \
    .mode("overwrite") \
    .csv(f"gs://phivolcs_earthquake_data/temp_data/{file_name2}")"""


# save the dataframe as a table in big query, it uses a bucket to stage the file before creating the "temp_table" in big query
# this also create or replace existing temp_table
df.write \
    .format("bigquery") \
    .mode("overwrite") \
    .option("table", "finalproject-456408.earthquake_dataset.temp_table") \
    .option("temporaryGcsBucket", "your-temporary-bucket") \
    .option("temporaryGcsBucket", "phivolcs_earthquake_data") \
    .option("parentProject", "finalproject-456408") \
    .save()



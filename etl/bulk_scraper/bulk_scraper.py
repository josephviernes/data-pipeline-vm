#!/usr/bin/env python
# coding: utf-8

import os
from bs4 import BeautifulSoup as bs
import requests
import urllib3
from datetime import datetime
import csv
import io
from google.cloud import storage

def main():
    bucket = os.environ.get("bucket")
    folder = os.environ.get("folder")

    # PHIVOLCS Earthquake URL
    years = os.getenv("years").split(",")
    months = os.getenv("months").split(",")

    # Disable SSL warnings (since we're ignoring SSL verification)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # get the date, convert the month number to month name, get the current year, hour and minute
    now = datetime.now()
    day = now.day
    hour = f"{now.hour:02d}"
    minute = f"{now.minute:02d}"

    data_year = None
    data_month = None
    earthquake_data_compiled = []
    for year in years:
        for month in months:
            url = f"https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/{year}/{year}_{month}.html"

            raw_data = fetch_data(url)

            if raw_data is not None:           
                earthquake_data_monthly = parse_data(raw_data)
                earthquake_data_compiled.extend(earthquake_data_monthly)
                data_year = year
                data_month = month

            else:
                continue
        

            print(f"adding {month}-{year} data to the list...")


    upload_to_gcs(
            earthquake_data=earthquake_data_compiled,
            bucket_name=bucket,
            destination_blob_name=f"{folder}/{data_year}_{data_month}_{day}_{hour}_{minute}_earthquake_data.csv"         
        )


def fetch_data(url):
    try:
        # Fetch the webpage with SSL verification disabled
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Raise an error if request fails (e.g., 404, 500)
        return bs(response.content)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")


def parse_data(soup):
    # This will create a list that will store the extracted data
    earthquake_data = []

    # Extract the table containing the earthquake data, skips the first 2 tables [0:1]
    table = soup.body.find_all("table", class_="MsoNormalTable")[2]


    # Refine the HTML data (only data with <tr> tag, also skips the header[0:1]
    rows = table.find_all("tr")[2:]

    # This will extract the data contained in <td> tags that is within the <tr> tag. The data in the <td> tag is then stored in the earthquake_data list
    for row in rows:
        cells = row.find_all("td")

        date_time = cells[0].text.strip()
        latitude = cells[1].text.strip()
        longitude = cells[2].text.strip()
        depth = cells[3].text.strip()
        magnitude = cells[4].text.strip()
        relative_location = cells[5].text.strip()

        earthquake_data.append([date_time, latitude, longitude, depth, magnitude, relative_location])

    return earthquake_data



def upload_to_gcs(earthquake_data, bucket_name, destination_blob_name):
    # Create a storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    #Create a temporary memory of CSV
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    # Write headers; ensures that the list of list (earthquake_data) is saved seamlessly as csv
    writer.writerow(['datetime', 'latitude', 'longitude', 'depth_km', 'magnitude', 'relative_location'])

    # Write the rows or list of earthquake_data
    writer.writerows(earthquake_data)

    # Reset buffer to beginning
    csv_buffer.seek(0)

    # Upload the file to GCS
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"List uploaded as CSV to gs://{bucket_name}/{destination_blob_name}")


if __name__ == "__main__":
    main()


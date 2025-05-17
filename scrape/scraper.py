#!/usr/bin/env python
# coding: utf-8

from bs4 import BeautifulSoup as bs
import requests
import urllib3
import time
from datetime import datetime, timedelta
import calendar
import csv
import io
from google.cloud import storage

# Disable SSL warnings (since we're ignoring SSL verification)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# get the month
now = datetime.now()
current_month = now.month

# convert the month number to month name, get the current year and number
month = now.strftime("%B")
year = now.year
day = now.day
hour = f"{now.hour:02d}"
minute = f"{now.minute:02d}"

# This will create a list that will store the extracted data
earthquake_data = []


# PHIVOLCS Earthquake URL
url_link = "https://earthquake.phivolcs.dost.gov.ph/" 
# f"https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/{year}/{year}_{month}.html"

try:
    # Fetch the webpage with SSL verification disabled
    response = requests.get(url_link, verify=False)
    response.raise_for_status()  # Raise an error if request fails (e.g., 404, 500)
    soup = bs(response.content)

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")

if soup:
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




def upload_to_gcs(bucket_name, destination_blob_name, credentials_path):
    # Create a storage client
    storage_client = storage.Client.from_service_account_json(credentials_path)
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

upload_to_gcs(
    bucket_name="phivolcs_earthquake_data",
    destination_blob_name=f"dailies/{year}_{month}_{day}_{hour}_{minute}_earthquake_data.csv",
    credentials_path="/home/joseph/Documents/dez_final_project/gsa/finalproject-456408-a18af71e91f6.json"
)

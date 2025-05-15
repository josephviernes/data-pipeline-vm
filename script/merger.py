#!/usr/bin/env python
# coding: utf-8

import os
from google.cloud import bigquery


# trigger the big query PROCEDURE (update_earthquake) - put a unique id to temp_table as well as merge it to the main table

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/joseph/Documents/dez_final_project/gsa/finalproject-456408-a18af71e91f6.json"
# Set up authentication (assuming ADC is used)
client = bigquery.Client()

# SQL query to call the stored procedure
query = f"""
    CALL `finalproject-456408.earthquake_dataset.update_earthquakes`()
    """
# Execute the query
client.query(query).result() 




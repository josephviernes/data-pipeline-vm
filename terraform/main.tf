terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.42.0"
    }
  }
}

provider "google" {
  project = "earthquake-etl"
  region  = "ASIA"
}


resource "google_storage_bucket" "earthquake-data-bucket" {
  name          = "earthquake-etl-bucket"
  location      = "asia-southeast1"
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "earthquake_data_dataset" {
  dataset_id = "earthquake_etl_dataset"
  location = "asia-southeast1"
}


resource "google_bigquery_table" "earthquake_table" {
  dataset_id = google_bigquery_dataset.earthquake_data_dataset.dataset_id
  table_id   = "phivolcs_earthquake"
  project    = "earthquake-etl"

  schema = jsonencode([
    {
      name = "date_time"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "latitude"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "longitude"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "depth"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "magnitude"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "relative_location"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "province"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "id"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "location_point"
      type = "GEOGRAPHY"
      mode = "NULLABLE"
    }
  ])
}
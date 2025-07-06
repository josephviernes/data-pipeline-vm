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
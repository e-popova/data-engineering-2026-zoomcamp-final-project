variable "credentials" {
  description = "My Credentials"
  #Update the path to service account API key json
  default     = "../keys/sa_key.json"
}


variable "project" {
  description = "Project"
  #Update the below to your Google Cloud Project ID
  default     = "de-zoomcamp2026-final"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-north1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "EU"
}

variable "bq_dataset_raw_name" {
  description = "Raw data"
  #Update the below to your desired dataset name for raw data
  default     = "crypto_raw_test"
}

variable "gcs_bucket_name" {
  description = "GC Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "crypto-data-full-test"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
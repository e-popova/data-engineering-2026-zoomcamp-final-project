variable "credentials" {
  description = "My Credentials"
  #Update the below to your path to service account API key
  default     = "keys/sa_terraform_key.json"
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

variable "bq_dataset_staging_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to your desired dataset name
  default     = "crypto_staging"
}

variable "bq_dataset_marts_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to your desired dataset name
  default     = "crypto_marts"
}

variable "gcs_bucket_name" {
  description = "GC Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "crypto-data-full"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
variable "bucket" {
  description = "Name of the data lake bucket"
  type        = string
}

# No default value provided, so it needs to be assigned at runtime
variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
}

variable "bq_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
}

# Create locals block
locals {
    data_lake_bucket= var.bucket
}
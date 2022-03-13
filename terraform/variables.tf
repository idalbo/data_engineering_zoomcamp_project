locals {
  data_lake_bucket = "dtc_data_lake_project"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset hsoting raw data"
  type = string
  default = "dtc_de_project_raw"
}

# variable "BQ_DATASET_PROD" {
#   description = "BigQuery Dataset hosting production data"
#   type = string
#   default = "dtc_de_project_prod"
# }
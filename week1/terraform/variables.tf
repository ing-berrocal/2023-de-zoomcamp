
locals {
    data_lake_bucket = "dtc_data_lake"
}

variable "project" {
    description = "Your GCP Project ID"
}

variable "region" {
    description = "Region for GCP resources. "
    default = "us-east1"
    type = string
}

variable "bucket_name" {
    description = "The name of the google cloud storage. Must be globally unique. "
    default = "dbn_bucket_cgp_test"
    type = string
}

variable "storage_class" {
    description = "Storage class type for your bucket. Check official docks for more info"
    default = "STANDARD"
    type = string
}

variable "BQ_DATASET" {
    description = "BigQuery Dataset"
    default = "trips_data_all"
    type = string
}

variable "TABLE_NAME" {
    description = "BigQuery Table"
    type = string
    default = "ny_trip"
}
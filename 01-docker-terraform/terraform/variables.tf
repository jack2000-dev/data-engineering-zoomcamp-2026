variable "credentials" {
  description = "GCP Credentials"
  default     = "./keys/terraform-demo-497607-317435842dd5.json"
}

variable "project" {
  description = "Project"
  default     = "terraform-demo-497607"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default = "terraform_demo_497607_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default = "terraform-demo-497607-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Sotrage Class"
  default = "STANDARD"
}
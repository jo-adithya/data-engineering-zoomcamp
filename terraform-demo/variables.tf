variable "credentials_path" {
  description = "The path to the service account key file"
  default     = "./keys/my-creds.json"

}

variable "project_id" {
  description = "The project ID to deploy to"
  default     = "terraform-demo-412611"
}

variable "region" {
  description = "The region to deploy to"
  default     = "us-central1"
}

variable "location" {
  description = "The location of the resources"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My big query dataset name"
  default     = "demo_dataset"
}

variable "gcp_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "gcp_bucket_name" {
  description = "My storage bucket name"
  default     = "terraform-demo-412611-terra-bucket"
}

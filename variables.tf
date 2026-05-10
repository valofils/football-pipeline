variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Deployment environment tag"
  type        = string
  default     = "dev"
}

variable "project" {
  description = "Project prefix for resource names"
  type        = string
  default     = "football-pipeline"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for the Parquet lake"
  type        = string
  # Override in terraform.tfvars — must be globally unique
}

variable "db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "football"
}

variable "db_username" {
  description = "RDS master username"
  type        = string
  default     = "football_user"
}

variable "db_password" {
  description = "RDS master password — supply via TF_VAR_db_password env var"
  type        = string
  sensitive   = true
}

variable "local_cidr" {
  description = "Your local IP in CIDR notation (e.g. 1.2.3.4/32) for RDS access. Leave empty to skip."
  type        = string
  default     = ""
}

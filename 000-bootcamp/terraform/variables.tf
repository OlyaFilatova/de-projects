variable "aws_region" {
  description = "AWS region used by LocalStack"
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key" {
  description = "Fake AWS access key for LocalStack"
  type        = string
  default     = "test"
}

variable "aws_secret_key" {
  description = "Fake AWS secret key for LocalStack"
  type        = string
  default     = "test"
  sensitive   = true
}

variable "localstack_endpoint" {
  description = "LocalStack API endpoint"
  type        = string
  default     = "http://localhost:4566"
}

variable "raw_bucket_name" {
  description = "S3 bucket for raw OSINT data"
  type        = string
  default     = "osint-raw"
}

variable "glue_database_name" {
  description = "Glue Catalog database name"
  type        = string
  default     = "osint"
}

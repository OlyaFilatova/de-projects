output "s3_bucket_name" {
  description = "Name of the OSINT S3 bucket"
  value       = aws_s3_bucket.osint_raw.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the OSINT S3 bucket"
  value       = aws_s3_bucket.osint_raw.arn
}

output "s3_bucket_url" {
  description = "LocalStack S3 endpoint"
  value       = "${var.localstack_endpoint}/${aws_s3_bucket.osint_raw.bucket}"
}

# output "glue_database_name" {
#   description = "Glue Catalog database"
#   value       = aws_glue_catalog_database.osint.name
# }

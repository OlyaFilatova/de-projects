# TODO: study terraform
# ============================================================
# S3 BUCKET
# ============================================================

resource "aws_s3_bucket" "osint_raw" {
  bucket = var.raw_bucket_name
}

# ============================================================
# S3 PUBLIC ACCESS BLOCK
# ============================================================

resource "aws_s3_bucket_public_access_block" "osint_raw" {
  bucket = aws_s3_bucket.osint_raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================
# S3 LIFECYCLE
# ============================================================

resource "aws_s3_bucket_lifecycle_configuration" "osint_raw" {
  bucket = aws_s3_bucket.osint_raw.id

  rule {
    id     = "expire-temporary-data"
    status = "Enabled"

    filter {
      prefix = "temporary/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "archive-raw-data"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    # Note:
    # STANDARD_IA behavior may not be fully emulated
    # by LocalStack. This configuration is useful when
    # eventually deploying to real AWS.
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

# ============================================================
# GLUE CATALOG DATABASE (not available in LocalStack)
# ============================================================

# resource "aws_glue_catalog_database" "osint" {
#   name = var.glue_database_name
# }

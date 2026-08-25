# Single bucket holding all three medallion layers under separate prefixes
# (bronze/, silver/, gold/) rather than three buckets -- simpler IAM and
# keeps the free-tier 5GB budget in one place to reason about.

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bronze_bucket_name

  tags = {
    Project = var.project_name
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Disabled" # keep well within the 5GB free-tier budget
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "bronze_bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

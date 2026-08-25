# Glue Data Catalog acts as the metastore Databricks reads from (Community
# Edition has no Unity Catalog) and lets dbt-databricks resolve source
# tables by name instead of raw S3 paths.

resource "aws_glue_catalog_database" "lakehouse" {
  name = "${var.project_name}_lakehouse"
}

resource "aws_iam_role" "glue_crawler" {
  name = "${var.project_name}-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_read" {
  name = "${var.project_name}-glue-s3-read"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.lakehouse.arn,
        "${aws_s3_bucket.lakehouse.arn}/bronze/*",
      ]
    }]
  })
}

resource "aws_glue_crawler" "bronze" {
  name          = "${var.project_name}-bronze-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.lakehouse.name

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/transactions/"
  }

  # Free-tier-friendly: run on demand (via Dagster) rather than on a
  # schedule, so crawler runs only happen when there's actually new data.
  schedule = null

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

output "glue_database_name" {
  value = aws_glue_catalog_database.lakehouse.name
}

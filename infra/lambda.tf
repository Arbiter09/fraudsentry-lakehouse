# Packaging: this expects a pre-built zip at ../build/lambda.zip containing
# lambda/handler.py + the common/ package (see infra/README.md for the
# build command -- Terraform doesn't do Python dependency bundling itself).

resource "aws_sqs_queue" "bronze_dlq" {
  name                      = "${var.project_name}-bronze-dlq"
  message_retention_seconds = 1209600 # 14 days, SQS free-tier friendly
}

resource "aws_iam_role" "lambda_bronze" {
  name = "${var.project_name}-lambda-bronze-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_logs" {
  role       = aws_iam_role.lambda_bronze.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_bronze_permissions" {
  name = "${var.project_name}-lambda-bronze-permissions"
  role = aws_iam_role.lambda_bronze.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${aws_s3_bucket.lakehouse.arn}/bronze/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = [aws_sqs_queue.bronze_dlq.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "kafka:DescribeCluster",
          "kafka:GetBootstrapBrokers",
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeGroup",
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:ReadData",
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_lambda_function" "bronze_ingest" {
  function_name = "${var.project_name}-bronze-ingest"
  role          = aws_iam_role.lambda_bronze.arn
  # handler.py sits at the zip root -- NOT under a lambda/ directory,
  # since `lambda` is a Python keyword and unimportable as a module path.
  # See infra/build_lambda.sh.
  handler          = "handler.handler"
  runtime          = "python3.12"
  filename         = "${path.module}/../build/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../build/lambda.zip")
  timeout          = 30
  memory_size      = 256 # smallest practical size, stays well within free-tier compute

  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.lakehouse.bucket
      DLQ_URL       = aws_sqs_queue.bronze_dlq.id
    }
  }
}

# Event-source mapping only makes sense while the MSK cluster exists.
resource "aws_lambda_event_source_mapping" "msk_trigger" {
  count             = var.enable_msk ? 1 : 0
  event_source_arn  = aws_msk_cluster.this[0].arn
  function_name     = aws_lambda_function.bronze_ingest.arn
  topics            = ["transactions"]
  starting_position = "LATEST"
  batch_size        = 100
}

output "lambda_function_name" {
  value = aws_lambda_function.bronze_ingest.function_name
}

# infra/ -- Terraform

Provisions S3 (bronze/silver/gold), Glue (crawler + Data Catalog), Lambda
(bronze ingestion), and optionally MSK.

## Cost notes

- S3, Glue, Lambda, SQS are all designed to stay within AWS free-tier
  limits under normal demo usage.
- **MSK is not free-tier eligible** and is gated behind `enable_msk`
  (default `false`). Only turn it on for an active demo session, and
  destroy it immediately after -- see the comment at the top of `msk.tf`
  for the exact commands.

## Usage

```bash
# 1. Package the Lambda (handler.py + the shared common/ module)
mkdir -p build/lambda_pkg
cp -r ../lambda/handler.py build/lambda_pkg/
mkdir -p build/lambda_pkg/lambda build/lambda_pkg/common
cp ../lambda/handler.py build/lambda_pkg/lambda/handler.py
cp ../common/*.py build/lambda_pkg/common/
cd build/lambda_pkg && zip -r ../lambda.zip . && cd ../..

# 2. Provision S3 + Glue + Lambda (no MSK)
terraform init
terraform apply -var bronze_bucket_name=<globally-unique-bucket-name>

# 3. (demo only) also stand up MSK
terraform apply -var enable_msk=true -var bronze_bucket_name=<same-bucket-name>

# 4. Tear MSK down as soon as the demo is done
terraform destroy -target=aws_msk_cluster.this \
  -var enable_msk=true -var bronze_bucket_name=<same-bucket-name>
```

Run `terraform validate` and `terraform plan` before every `apply` --
this repo's Terraform hasn't been run against a real AWS account yet in
this environment, so treat the first `plan` as the real correctness
check.

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix used for naming resources"
  type        = string
  default     = "fraudsentry"
}

variable "bronze_bucket_name" {
  description = "S3 bucket for the bronze layer. Must be globally unique -- override in a .tfvars file."
  type        = string
}

variable "enable_msk" {
  description = <<-EOT
    Whether to provision the MSK cluster. MSK has no free tier (it bills
    per broker-hour), so this defaults to false. Set to true only for a
    short demo session, then run `terraform apply -var enable_msk=false`
    (or `terraform destroy -target=aws_msk_cluster.this`) immediately
    after to avoid ongoing cost. See infra/msk.tf and the README.
  EOT
  type        = bool
  default     = false
}

variable "stage" {
  description = "Environment stage (e.g. dev)"
  type = string
  default = "dev"
}

variable "aws_region" {
  description = "AWS region to use"
  type = string
  default = "eu-central-1"
}

variable "aws_profile_default" {
  description = "AWS CLI profile for default provider"
  type = string
  default = "default"
}

variable "aws_profile_dev" {
  description = "AWS CLI profile for dev provider (optional)"
  type = string
  default = "developer-dev"
}

variable "trusted_principal_arn" {
  description = "ARN of the user or role allowed to assume this role"
  type        = string
}

variable "iam_policy_arns" {
  description = "List of IAM policy ARNs to attach to the role"
  type = list(string)
  default = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/AmazonRDSFullAccess",
    "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
  ]
}

variable "db_secret_id" {
  description = "Secrets Manager secret id containing DB credentials (JSON with username/password)"
  type = string
  default = "dev/db_credentials"
}

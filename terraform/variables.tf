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

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
}
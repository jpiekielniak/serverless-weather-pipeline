variable "stage" {
  description = "Deployment stage (e.g. dev, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region where resources are deployed"
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
}

variable "service_name" {
  default = "weather-pipeline"
}
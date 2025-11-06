variable "stage" {
  description = "Environment name (dev, prod, etc.)"
  type = string
}

variable "trusted_principal_arn" {
  description = "ARN of the trusted AWS principal allowed to assume this role"
  type        = string
  sensitive   = true
}



variable "policy_arns" {
  description = "List of IAM policy ARNs to attach to the role"
  type = list(string)
}

variable "stage" {
  type = string
  default = "dev"
}

variable "publicly_accessible" {
  type = bool
  default = true
}

variable "db_secret_id" {
  description = "Secrets Manager secret id containing DB credentials (JSON with username/password)"
  type = string
  default = "dev/db_credentials"
}


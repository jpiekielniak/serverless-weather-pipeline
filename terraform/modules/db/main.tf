data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = var.db_secret_id
}

locals {
  parsed_secret = try(jsondecode(data.aws_secretsmanager_secret_version.db_credentials.secret_string), {})
  db_username_from_secret  = lookup(local.parsed_secret, "username", "")
  db_password_from_secret  = lookup(local.parsed_secret, "password", "")
  db_name_from_secret = lookup(local.parsed_secret, "db_name", "")
}

resource "aws_db_subnet_group" "this" {
  name = "${var.stage}-rds-subnet-group"
  description = "Subnet group for RDS ${var.stage}"
  subnet_ids = data.aws_subnets.default.ids

  tags = {
    Name = "${var.stage}-rds-subnet-group"
  }
}

resource "aws_db_instance" "this" {
  identifier = "${var.stage}-database"
  engine = "postgres"
  engine_version = "17.6"
  instance_class = "db.t3.micro"
  allocated_storage = 20
  db_name = local.db_name_from_secret
  username = local.db_username_from_secret != "" ? local.db_username_from_secret : local.db_username_from_secret
  password = local.db_password_from_secret != "" ? local.db_password_from_secret : ""
  skip_final_snapshot = true
  publicly_accessible = var.publicly_accessible
  storage_encrypted = false
  db_subnet_group_name = aws_db_subnet_group.this.name

  tags = {
    Environment = var.stage
    Name = "${var.stage}-database"
  }
}

module "db" {
  source = "./modules/db/"
  stage = var.stage
}

module "s3" {
  source = "./modules/s3"
}

module "iam" {
  source         = "./modules/iam"

  aws_account_id = var.aws_account_id
  aws_region     = var.aws_region
  stage          = var.stage
}

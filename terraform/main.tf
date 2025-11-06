module "iam_role" {
  source = "./modules/iam-role"
  stage  = var.stage
  policy_arns = var.iam_policy_arns
  trusted_principal_arn = var.trusted_principal_arn
}

module "db" {
  source = "./modules/db/"
  stage = var.stage
}

module "s3" {
  source = "./modules/s3"
}

resource "aws_iam_policy" "lambda_secrets_access" {
  name        = "LambdaSecretsManagerAccess-${var.stage}"
  description = "Allow Lambda to read dev/api_keys secrets from Secrets Manager"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "secretsmanager:GetSecretValue"
        ],
        Resource = [
        "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:${var.stage}/api_keys*",
        "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:${var.stage}/db_credentials*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_secrets_attachment" {
  role       = "serverless-weather-pipeline-${var.stage}-${var.aws_region}-lambdaRole"
  policy_arn = aws_iam_policy.lambda_secrets_access.arn
}

resource "aws_iam_policy" "lambda_s3_access" {
  name        = "LambdaS3Access-${var.stage}"
  description = "Allow Lambda to access S3 buckets for raw and processed weather data"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:PutObjectAcl",
          "s3:DeleteObject"
        ],
        Resource = [
          "arn:aws:s3:::weather-pipeline-raw-dev/*",
          "arn:aws:s3:::weather-pipeline-processed-dev/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        Resource = [
          "arn:aws:s3:::${var.service_name}-raw-${var.stage}",
          "arn:aws:s3:::${var.service_name}-processed-${var.stage}"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_attachment" {
  role       = "serverless-weather-pipeline-${var.stage}-${var.aws_region}-lambdaRole"
  policy_arn = aws_iam_policy.lambda_s3_access.arn
}

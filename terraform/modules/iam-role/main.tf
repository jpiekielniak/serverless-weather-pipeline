resource "aws_iam_role" "this" {
  name = "developer-${var.stage}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          AWS = var.trusted_principal_arn
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attachments" {
  for_each = toset(var.policy_arns)
  role = aws_iam_role.this.name
  policy_arn = each.value
}

output "role_arn" {
  value = aws_iam_role.this.arn
}

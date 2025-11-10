resource "aws_s3_bucket" "weather_pipeline_raw_dev" {
  bucket = "weather-pipeline-raw-dev"

  tags = {
    Name  = "weather-pipeline-raw-dev"
    Env   = "dev"
  }
}

resource "aws_s3_bucket" "weather_pipeline_processed_dev" {
  bucket = "weather-pipeline-processed-dev"

  tags = {
    Name = "weather-pipeline-processed-dev"
    Env = "dev"
  }
}

resource "aws_s3_bucket_public_access_block" "weather_pipeline_raw_dev_block" {
  bucket = aws_s3_bucket.weather_pipeline_raw_dev.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "weather_pipeline_processed_dev_block" {
  bucket = aws_s3_bucket.weather_pipeline_processed_dev.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# Serverless Weather Pipeline

A production-ready, serverless data pipeline that fetches current weather data for configured cities, stores raw payloads in S3, aggregates daily metrics, and persists results to PostgreSQL on AWS RDS. The stack uses AWS Lambda (Python 3.12), S3, Secrets Manager, IAM, and RDS, provisioned with Terraform and deployed via the Serverless Framework.

- Runtime: Python 3.12 (async-first, aiohttp/aioboto3, SQLAlchemy)
- Infra: AWS Lambda, S3 (raw + processed), RDS Postgres, Secrets Manager, IAM
- Provisioning: Terraform (S3 buckets, IAM policies, RDS)
- Deployment: Serverless Framework v4 (scheduled Lambdas, shared layer)
- CI-ish tooling: pytest, mypy, flake8 (79 cols), black, isort, pre-commit

## Architecture

- Lambdas
  - fetchWeather (hourly):
    - Reads secrets and config (API URL, API key, bucket names, DB URL)
    - Fetches current weather per city from OpenWeatherMap
    - Writes raw JSON to S3: `raw/{city}/{YYYY}/{MM}/{DD}/{city}_{ISO}.json`
  - weatherDailyAggregator (daily):
    - Lists raw files in S3 for each city/day
    - Aggregates metrics (min/max/avg temp, humidity, precipitation, wind)
    - Writes processed JSON to S3: `processed/{city}/{YYYY}/{MM}/{DD}/...`
    - Upserts daily aggregate into Postgres (RDS)

- Shared Layer: `layers/common` (bundled third-party dependencies)
- Database: PostgreSQL on RDS. SQLAlchemy models live under `src/app/models/`.

Cron (UTC by default in AWS):
- fetchWeather: `cron(0 * * * ? *)` (co godzina)
- weatherDailyAggregator: `cron(5 23 * * ? *)` (codziennie 23:05)

## Repository layout

- `src/app/lambdas/fetcher/handler.py`: fetch hourly weather -> S3
- `src/app/lambdas/weather_daily_aggregator/handler.py`: aggregate per day
- `src/app/services/`: async services (S3, DB, logger, OWM API, secrets)
- `src/app/models/`: SQLAlchemy models (City, Location, WeatherAggregate)
- `layers/common/`: packaged deps for Lambda layer
- `terraform/`: IaC modules for S3, IAM, and RDS
- `sql/weather_seed.sql`: optional seed data for cities
- `tests/`: unit tests (pytest + pytest-asyncio)

## Prerequisites

Install the following locally:

- Python 3.12
- Node.js (LTS) + npm
- Serverless Framework CLI v4
- Terraform ≥ 1.6
- AWS CLI v2 (configured profile with access to target account)
- PostgreSQL client (psql) – optional, for seeding or debugging

Example installs on macOS (Homebrew):

```bash
brew install python@3.12 terraform awscli
npm install -g serverless
```

Ensure AWS CLI works and a default profile is configured:

```bash
aws sts get-caller-identity
```

## Secrets and configuration (AWS)

This project expects the following AWS Secrets Manager secrets (under your chosen `stage`, e.g. `dev`). All Lambdas have IAM to read:

- `${stage}/api_keys` (JSON) – OpenWeatherMap key:
  ```json
  { "openweathermap": "YOUR_OPENWEATHERMAP_API_KEY" }
  ```
- `${stage}/db_credentials` (JSON) – DB credentials and connection URL:
  ```json
  {
    "username": "db_user",
    "password": "db_pass",
    "db_name": "weather",
    "db_url": "postgresql+pg8000://db_user:db_pass@<rds-endpoint>:5432/weather"
  }
  ```

Notes:
- The Terraform DB module reads `username`, `password`, and `db_name` from `${stage}/db_credentials` to create the RDS instance.
- After Terraform creates RDS, update the same secret to include the final `db_url` with the actual RDS endpoint. Lambdas read `db_url` at runtime.

Optional: Create secrets via CLI (example for `dev`):

```bash
aws secretsmanager create-secret \
  --name dev/api_keys \
  --secret-string '{"openweathermap":"YOUR_KEY"}'

aws secretsmanager create-secret \
  --name dev/db_credentials \
  --secret-string '{"username":"db_user","password":"db_pass","db_name":"weather"}'
```

## Infrastructure: Terraform

Variables are defined in `terraform/variables.tf`. Provide required values via `terraform/secrets.tfvars` (an example file exists):

`terraform/secrets.tfvars` example:
```hcl
trusted_principal_arn = "arn:aws:iam::<account-id>:user/<user-or-role>"
aws_region     = "eu-central-1"
aws_account_id = "<account-id>"
stage          = "dev"
```

Apply Terraform:

```bash
cd terraform
terraform init
terraform apply -var-file=secrets.tfvars
```

What Terraform does:
- Creates two S3 buckets: `weather-pipeline-raw-dev` and `weather-pipeline-processed-dev`
- Attaches IAM policies allowing Lambda access to the above buckets and secrets `${stage}/api_keys` and `${stage}/db_credentials`
- Provisions an RDS PostgreSQL instance in the default VPC (publicly accessible in the example; adjust in production)

After apply:
1) Retrieve RDS endpoint from AWS Console or via CLI, and update the `${stage}/db_credentials` secret to include `db_url`.
2) Optionally seed DB (see Seeding below).

## Application deployment: Serverless Framework

Serverless reads environment via `--param` flags (see `serverless.yml`). Use the buckets and secret names configured above.

Deploy (example for `dev`):

```bash
sls deploy \
  --stage dev \
  --param openweathermap-api-url=https://api.openweathermap.org/data/2.5 \
  --param secret-name-api=dev/api_keys \
  --param secret-name-db=dev/db_credentials \
  --param raw-bucket-name=weather-pipeline-raw-dev \
  --param processed-bucket-name=weather-pipeline-processed-dev
```

Invoke locally (examples):

- Fetcher (no event body required):
  ```bash
  sls invoke local -f fetchWeather --stage dev \
    --param openweathermap-api-url=https://api.openweathermap.org/data/2.5 \
    --param secret-name-api=dev/api_keys \
    --param secret-name-db=dev/db_credentials \
    --param raw-bucket-name=weather-pipeline-raw-dev \
    --param processed-bucket-name=weather-pipeline-processed-dev
  ```

- Daily aggregator for a specific date:
  ```bash
  sls invoke local -f weatherDailyAggregator --stage dev \
    --data '{"date":"2025-01-01"}' \
    --param openweathermap-api-url=https://api.openweathermap.org/data/2.5 \
    --param secret-name-api=dev/api_keys \
    --param secret-name-db=dev/db_credentials \
    --param raw-bucket-name=weather-pipeline-raw-dev \
    --param processed-bucket-name=weather-pipeline-processed-dev
  ```

## Local development

Create a virtual environment and install deps:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements/dev.txt
```

Run tests:

```bash
pytest -q
```

Lint/format/type-check:

```bash
black .
isort .
flake8
mypy
```

Pre-commit hooks (optional but recommended):

```bash
pre-commit install
pre-commit run -a
```

## Database

- ORM models: see `src/app/models/`
- Alembic scaffolding exists under `src/app/alembic/` (migrations are not packaged to Lambda)
- Example seed: `sql/weather_seed.sql`

Seed data (example with psql):

```bash
# Assuming you exported DB URL
export DATABASE_URL="postgresql://db_user:db_pass@host:5432/weather"
psql "$DATABASE_URL" -f sql/weather_seed.sql
```

## Configuration summary (Serverless env)

The following environment variables are provided to Lambdas via `serverless.yml` params:

- `API_URL` ← `--param openweathermap-api-url`
- `SECRET_NAME_API` ← Secrets Manager name for API keys (e.g. `dev/api_keys`)
- `RAW_BUCKET_NAME` ← raw S3 bucket (e.g. `weather-pipeline-raw-dev`)
- `PROCESSED_BUCKET_NAME` ← processed S3 bucket (e.g. `weather-pipeline-processed-dev`)
- `SECRET_NAME_DB` ← Secrets Manager name with `db_url` (e.g. `dev/db_credentials`)

## Costs and security

- The sample Terraform uses a publicly accessible RDS instance for simplicity. In production, place RDS in private subnets and run Lambdas within a VPC.
- S3 buckets have public access blocked.
- Secrets are read at runtime; scope IAM policies minimally for least privilege.

## Troubleshooting

- Secrets not found / AccessDenied:
  - Verify secret names match the `--param` values and IAM policies include `${stage}/api_keys` and `${stage}/db_credentials`.
- Cannot connect to DB:
  - Ensure `db_url` in secret is correct and RDS security allows connectivity from Lambda (if using VPC) or is publicly reachable (sample config).
- Missing buckets or wrong names:
  - Confirm Terraform applied successfully and use those names in Serverless params.
- Flake8 E501 line too long:
  - Project enforces 79-char lines (see `pyproject.toml`). Wrap docstrings and strings accordingly.

## License

MIT (or your preferred license). Update this section as needed.

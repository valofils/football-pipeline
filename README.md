# football-pipeline-v7 — AWS (S3 + RDS) + Terraform

Lifts the pipeline into AWS. Adds one new skill layer on top of v6.

```
v6 stack  +  S3 (Parquet lake)  +  RDS PostgreSQL  +  Terraform (IaC)
```

Local Docker services unchanged: Kafka, Spark, Airflow, moto-server (local S3 mock).

---

## What changed vs v6

| Component | v6 | v7 |
|---|---|---|
| Parquet lake | `data/parquet/` (local volume) | `s3a://bucket/parquet/` (S3) |
| PostgreSQL | `football-db` Docker container | AWS RDS db.t3.micro |
| Infrastructure | docker-compose only | docker-compose + Terraform |
| New fixture | — | `s3_bucket` (moto mock) |
| New test class | 7 classes / 35 tests | 8 classes / 40 tests |
| New helpers | — | `s3a_path()` in spark_utils |
| New JARs | `postgresql.jar` | + `hadoop-aws.jar` + `aws-java-sdk-bundle.jar` |

---

## Prerequisites

- Docker + Docker Compose
- Terraform ≥ 1.7 (`brew install terraform` / `apt install terraform`)
- AWS CLI configured (`aws configure` or env vars)
- Python 3.12 virtual environment

---

## Quick start

### 1 — Download JARs

```bash
chmod +x scripts/get_jars.sh
./scripts/get_jars.sh
```

### 2 — Provision AWS infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: set bucket_name, local_cidr
export TF_VAR_db_password="your-secure-password"

terraform init
terraform plan
terraform apply          # ~5 min for RDS
```

Note the outputs:

```
s3_bucket_name = "football-pipeline-parquet-lake-xyz"
rds_host       = "football-pipeline-db-dev.xxxx.eu-west-1.rds.amazonaws.com"
rds_port       = 5432
```

### 3 — Bootstrap the RDS schema

```bash
export PGPASSWORD="your-secure-password"
psql -h <rds_host> -U football_user -d football -f scripts/bootstrap_rds.sql
```

### 4 — Configure environment

Create a `.env` file in the project root (git-ignored):

```bash
# .env
FOOTBALL_DB_HOST=<rds_host from terraform output>
FOOTBALL_DB_PORT=5432
FOOTBALL_DB_NAME=football
FOOTBALL_DB_USER=football_user
FOOTBALL_DB_PASSWORD=your-secure-password

AWS_ACCESS_KEY_ID=<your key>
AWS_SECRET_ACCESS_KEY=<your secret>
AWS_DEFAULT_REGION=eu-west-1
S3_BUCKET_NAME=<bucket_name from terraform output>
```

### 5 — Start local services

```bash
docker compose --env-file .env up -d
```

### 6 — Publish test data to Kafka

```bash
python kafka/producer/produce_matches.py data/raw/matches.csv
```

### 7 — Trigger the DAG

Open Airflow at http://localhost:8080 (admin / admin) and trigger `football_pipeline`.

---

## Running tests

Tests use moto to mock S3 — no real AWS calls needed:

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=dags --cov=kafka --cov-report=term-missing
```

Expected: **40 tests, 100% pass**.

---

## Terraform teardown

```bash
cd terraform
terraform destroy
```

This destroys the RDS instance and S3 bucket. The bucket must be empty first:

```bash
aws s3 rm s3://<bucket_name> --recursive
terraform destroy
```

---

## Key mental models added in v7

### S3 as a data lake
- S3 stores objects under a flat key namespace; `/` in keys is just a naming convention
- Spark's `s3a://` scheme (via `hadoop-aws`) treats S3 like a filesystem
- Partitioned Parquet writes create `season=2023-24/` prefixes — Spark pushes filters down to skip irrelevant prefixes at read time

### Terraform lifecycle
- `terraform init` → download provider plugins
- `terraform plan` → diff desired vs actual state (read-only)
- `terraform apply` → converge actual to desired; updates `terraform.tfstate`
- State file is the source of truth — never edit manually; commit to remote backend (S3 + DynamoDB lock) in production

### IAM least-privilege
- The pipeline role gets `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` only on the specific bucket ARN
- No `s3:*` wildcard — avoids accidental cross-bucket access

### RDS vs Docker Postgres
- Same PostgreSQL 16 engine; wire protocol identical — psycopg2 and JDBC work unchanged
- RDS handles: automated backups, Multi-AZ failover, patching
- `publicly_accessible = true` + `local_cidr` variable lets you connect directly during dev; set `false` in production and route via a bastion or VPN

### moto for S3 unit tests
- `@mock_aws` intercepts all boto3 / botocore calls — no real HTTP to AWS
- `spark.hadoop.fs.s3a.endpoint` pointed at the moto server for Spark S3A calls
- Pattern: session-scoped bucket fixture → test writes Parquet → test reads it back

---

## Project structure

```
football-pipeline-v7/
├── terraform/
│   ├── main.tf                  # S3 bucket + RDS + IAM + VPC
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── dags/
│   ├── football_pipeline.py     # DAG: kafka_ingest >> validate >> load_postgres >> dbt_run
│   └── spark_utils.py           # SparkSession factory + s3a_path() helper
├── kafka/
│   ├── producer/produce_matches.py
│   └── consumer/consume_matches.py
├── dbt/                         # unchanged from v5/v6
├── tests/
│   ├── conftest.py              # + s3_bucket (moto) + aws_credentials fixtures
│   └── test_dag.py              # 40 tests, 8 classes
├── scripts/
│   ├── get_jars.sh              # downloads postgresql + hadoop-aws + aws-java-sdk-bundle
│   └── bootstrap_rds.sql        # DDL for RDS
├── spark/jars/                  # JAR files (git-ignored)
├── docker-compose.yaml          # + moto-server; football-db removed
└── requirements.txt
```

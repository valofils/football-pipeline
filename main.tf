terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── S3 ───────────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "parquet_lake" {
  bucket = var.bucket_name

  tags = {
    Project     = "football-pipeline"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "parquet_lake" {
  bucket = aws_s3_bucket.parquet_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "parquet_lake" {
  bucket = aws_s3_bucket.parquet_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "parquet_lake" {
  bucket = aws_s3_bucket.parquet_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "parquet_lake" {
  bucket = aws_s3_bucket.parquet_lake.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# ── IAM ──────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "pipeline_role" {
  name = "${var.project}-pipeline-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
      }
    ]
  })

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_iam_role_policy" "s3_access" {
  name = "s3-parquet-lake-access"
  role = aws_iam_role.pipeline_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.parquet_lake.arn,
          "${aws_s3_bucket.parquet_lake.arn}/*",
        ]
      }
    ]
  })
}

# ── VPC (minimal — two public subnets for RDS subnet group) ──────────────────

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true

  tags = { Name = "${var.project}-vpc-${var.environment}" }
}

resource "aws_subnet" "db_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = { Name = "${var.project}-db-a" }
}

resource "aws_subnet" "db_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = { Name = "${var.project}-db-b" }
}

resource "aws_db_subnet_group" "football" {
  name       = "${var.project}-db-subnet-group-${var.environment}"
  subnet_ids = [aws_subnet.db_a.id, aws_subnet.db_b.id]

  tags = { Project = var.project }
}

resource "aws_security_group" "rds" {
  name   = "${var.project}-rds-sg-${var.environment}"
  vpc_id = aws_vpc.main.id

  # Allow Postgres from within the VPC
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # Allow Postgres from your local machine (set via variable)
  dynamic "ingress" {
    for_each = var.local_cidr != "" ? [1] : []
    content {
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      cidr_blocks = [var.local_cidr]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-rds-sg" }
}

# ── RDS ──────────────────────────────────────────────────────────────────────

resource "aws_db_instance" "football" {
  identifier        = "${var.project}-db-${var.environment}"
  engine            = "postgres"
  engine_version    = "16.3"
  instance_class    = "db.t3.micro"   # free-tier eligible
  allocated_storage = 20
  storage_type      = "gp2"

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.football.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  publicly_accessible     = true   # set false in production
  skip_final_snapshot     = true   # set false in production
  deletion_protection     = false
  backup_retention_period = 7

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

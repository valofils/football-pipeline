output "s3_bucket_name" {
  description = "Name of the Parquet lake S3 bucket"
  value       = aws_s3_bucket.parquet_lake.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the Parquet lake S3 bucket"
  value       = aws_s3_bucket.parquet_lake.arn
}

output "rds_endpoint" {
  description = "RDS instance endpoint (host:port)"
  value       = aws_db_instance.football.endpoint
}

output "rds_host" {
  description = "RDS hostname (without port)"
  value       = aws_db_instance.football.address
}

output "rds_port" {
  description = "RDS port"
  value       = aws_db_instance.football.port
}

output "rds_db_name" {
  description = "Database name"
  value       = aws_db_instance.football.db_name
}

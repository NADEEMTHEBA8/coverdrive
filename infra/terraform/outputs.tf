# Outputs consumed by downstream automation (Airflow Connection setup,
# CI deploy steps, runbooks) and by humans inspecting `terraform output`.

output "lake_bucket_name" {
  description = "Name of the S3 lakehouse bucket. Set COVERDRIVE_S3_BUCKET to this value."
  value       = aws_s3_bucket.lake.bucket
}

output "lake_bucket_arn" {
  description = "ARN of the S3 lakehouse bucket — needed for IAM policy attachments."
  value       = aws_s3_bucket.lake.arn
}

output "access_logs_bucket_name" {
  description = "Name of the bucket storing S3 access logs."
  value       = aws_s3_bucket.access_logs.bucket
}

output "vpc_id" {
  description = "ID of the project VPC."
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs — use these when launching ECS tasks or RDS replicas."
  value       = aws_subnet.private[*].id
}

output "rds_endpoint" {
  description = "Connection endpoint for the Airflow metadata DB (host:port)."
  value       = aws_db_instance.airflow_metadata.endpoint
}

output "rds_database_name" {
  description = "Database name on the Airflow metadata instance."
  value       = aws_db_instance.airflow_metadata.db_name
}

output "ecr_repository_url" {
  description = "URL to push the pipeline container image to."
  value       = aws_ecr_repository.pipeline.repository_url
}

output "pipeline_task_role_arn" {
  description = "ARN of the IAM role the pipeline task assumes — attach to ECS task definitions."
  value       = aws_iam_role.pipeline_task.arn
}

output "task_execution_role_arn" {
  description = "ARN of the ECS task execution role — pulls images, writes ECS-agent logs."
  value       = aws_iam_role.task_execution.arn
}

output "pipeline_log_group_name" {
  description = "CloudWatch log group for pipeline output. Wire this into your ECS task definition."
  value       = aws_cloudwatch_log_group.pipeline.name
}

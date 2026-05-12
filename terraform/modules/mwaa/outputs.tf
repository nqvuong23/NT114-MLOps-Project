output "mwaa_environment_name" {
  description = "Name of the MWAA environment"
  value       = aws_mwaa_environment.main.name
}

output "mwaa_environment_arn" {
  description = "ARN of the MWAA environment"
  value       = aws_mwaa_environment.main.arn
}

output "mwaa_webserver_url" {
  description = "URL of the Airflow web server (PUBLIC_ONLY)"
  value       = aws_mwaa_environment.main.webserver_url
}

output "mwaa_execution_role_arn" {
  description = "ARN of the MWAA IAM execution role"
  value       = aws_iam_role.mwaa_execution.arn
}

output "mwaa_s3_bucket_name" {
  description = "Name of the S3 bucket used by MWAA for DAGs, plugins, and requirements"
  value       = aws_s3_bucket.mwaa.bucket
}

output "mwaa_s3_bucket_arn" {
  description = "ARN of the MWAA S3 bucket"
  value       = aws_s3_bucket.mwaa.arn
}

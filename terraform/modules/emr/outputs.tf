output "emr_application_id" {
  description = "ID of the EMR Serverless Spark application"
  value       = aws_emrserverless_application.spark.id
}

output "emr_application_arn" {
  description = "ARN of the EMR Serverless Spark application"
  value       = aws_emrserverless_application.spark.arn
}

output "emr_execution_role_arn" {
  description = "ARN of the IAM execution role used by EMR Serverless job runs"
  value       = aws_iam_role.emr_execution.arn
}

output "emr_execution_role_name" {
  description = "Name of the IAM execution role"
  value       = aws_iam_role.emr_execution.name
}

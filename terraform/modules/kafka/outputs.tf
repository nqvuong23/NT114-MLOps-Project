output "msk_serverless_cluster_arn" {
  description = "ARN of the Serverless MSK cluster"
  value       = aws_msk_serverless_cluster.main.arn
}

output "msk_serverless_cluster_name" {
  description = "Name of the Serverless MSK cluster"
  value       = aws_msk_serverless_cluster.main.cluster_name
}


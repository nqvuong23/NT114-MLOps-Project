output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "emr_serverless_security_group_id" {
  value = aws_security_group.emr_serverless.id
}

output "msk_serverless_security_group_id" {
  value = aws_security_group.msk_serverless.id
}

output "mwaa_security_group_id" {
  value = aws_security_group.mwaa.id
}

output "vpc_id" {
  value = aws_vpc.main.id
}

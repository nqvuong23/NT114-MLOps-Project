output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "data_processing_security_group_id" {
  value = aws_security_group.data_processing.id
}

output "vpc_id" {
  value = aws_vpc.main.id
}

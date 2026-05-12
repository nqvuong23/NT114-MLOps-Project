variable "project_name" {
  type        = string
  description = "Project name used as a prefix for all resource names"
}

variable "aws_region" {
  type        = string
  description = "AWS region"
}

variable "vpc_id" {
  type        = string
  description = "VPC ID from the networking module"
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "List of private subnet IDs for MWAA (at least 2 AZs required)"
}

variable "mwaa_security_group_ids" {
  type        = list(string)
  description = "Security group ID for the MWAA environment from the networking module"
}

variable "airflow_version" {
  type        = string
  description = "Apache Airflow version for the MWAA environment"
}

variable "environment_class" {
  type        = string
  description = "MWAA environment class: mw1.micro, mw1.small, mw1.medium, mw1.large, mw1.xlarge, mw1.2xlarge"
}

variable "min_workers" {
  type        = number
  description = "Minimum number of workers"
}

variable "max_workers" {
  type        = number
  description = "Maximum number of workers"
}

variable "schedulers" {
  type        = number
  description = "Number of schedulers"
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common tags to apply to all resources"
}

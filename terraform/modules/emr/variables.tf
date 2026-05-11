variable "project_name" {
  type        = string
  description = "Project name used as a prefix for all resource names"
}

variable "aws_region" {
  type        = string
  description = "AWS region"
}

variable "s3_bucket_arn" {
  type        = string
  description = "ARN of the S3 bucket imported from the storage module"
}

variable "msk_cluster_arn" {
  type        = string
  description = "ARN of the Serverless MSK cluster imported from the kafka module"
}

variable "release_label" {
  type        = string
  description = "EMR release label (e.g. emr-7.13.0)"
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common tags to apply to all resources"
}

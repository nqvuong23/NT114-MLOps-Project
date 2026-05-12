variable "project_name" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "ec2_ami_id" {
  type    = string
}

variable "ec2_instance_type" {
  type = string
}

variable "vpc_subnet_id" {
  type = string
}

variable "security_group_ids" {
  type = list(string)
}

variable "ec2_volume_type" {
  type    = string
}

variable "ec2_volume_size" {
  type    = number
}

variable "ec2_enable_detailed_monitoring" {
  type    = bool
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "s3_bucket_arn" {
  type = string
}

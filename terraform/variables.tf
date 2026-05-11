variable "project_name" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "environment" {
  type = string
}

# ------- Networking Module -------
variable "vpc_cidr" {
  type = string
}

variable "public_subnet_cidrs" {
  type = list(string)
}

variable "private_subnet_cidrs" {
  type = list(string)
}

variable "availability_zones" {
  type = list(string)
}

# ------- Storage Module -------
variable "s3_bucket_name" {
  type = string
}

variable "s3_versioning_status" {
  type = string
}

variable "s3_prefix_list" {
  type = list(string)
}

# ------- Computing Module -------
variable "ec2_ami_id" {
  type = string
}

variable "ec2_instance_type" {
  type = string
}

variable "ec2_volume_type" {
  type = string
}

variable "ec2_volume_size" {
  type = number
}

variable "ec2_enable_detailed_monitoring" {
  type = bool
}

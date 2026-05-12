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

# ------- EMR Serverless Module -------
variable "emr_release_label" {
  type = string
}

# ------- MWAA Module -------
variable "mwaa_airflow_version" {
  type        = string
  description = "Apache Airflow version for the MWAA environment"
}

variable "mwaa_environment_class" {
  type        = string
  description = "MWAA environment class (mw1.micro, mw1.small, mw1.medium, ...)"
}

variable "mwaa_min_workers" {
  type        = number
  description = "Minimum number of MWAA workers"
}

variable "mwaa_max_workers" {
  type        = number
  description = "Maximum number of MWAA workers"
}

variable "mwaa_schedulers" {
  type        = number
  description = "Number of MWAA schedulers"
}

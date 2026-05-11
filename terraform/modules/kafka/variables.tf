variable "project_name" {
  type        = string
  description = "Name of the project, used as prefix for all resource names"
}

variable "vpc_id" {
  type        = string
  description = "VPC ID imported from the networking module"
}

variable "public_subnet_ids" {
  type        = list(string)
  description = "List of public subnet IDs imported from the networking module (expects at least 2)"
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common tags to apply to all resources"
}

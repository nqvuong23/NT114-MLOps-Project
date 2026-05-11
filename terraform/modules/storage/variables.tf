variable "s3_bucket_name" {
  type = string
}

variable "s3_versioning_status" {
  type = string
}

variable "s3_prefix_list" {
  type = list(string)
}

variable "project_name" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_s3_bucket" "example" {
  bucket = var.s3_bucket_name

  tags = merge(var.tags, {
    Name = "${var.project_name}-bucket"
  })
}

resource "aws_s3_bucket_versioning" "versioning_example" {
  bucket = aws_s3_bucket.example.id
  versioning_configuration {
    status = var.s3_versioning_status
  }
}

resource "aws_s3_object" "object" {
  for_each = toset(var.s3_prefix_list)

  bucket  = aws_s3_bucket.example.id
  key     = each.value
  content = ""
}


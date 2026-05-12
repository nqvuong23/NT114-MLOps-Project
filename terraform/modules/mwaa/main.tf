data "aws_caller_identity" "current" {}

# -------------------------------------------------------
# S3 Bucket — MWAA DAGs and requirements
# -------------------------------------------------------
resource "aws_s3_bucket" "mwaa" {
  bucket        = "${var.project_name}-mwaa-${data.aws_caller_identity.current.account_id}"
  force_destroy = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-mwaa-bucket"
  })
}

# MWAA requires versioning to be enabled on the S3 bucket
resource "aws_s3_bucket_versioning" "mwaa" {
  bucket = aws_s3_bucket.mwaa.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Block all public access — MWAA bucket must NOT be public
resource "aws_s3_bucket_public_access_block" "mwaa" {
  bucket = aws_s3_bucket.mwaa.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Seed the required folder structure inside the bucket
resource "aws_s3_object" "dags_folder" {
  bucket  = aws_s3_bucket.mwaa.id
  key     = "dags/"
  content = ""
}

resource "aws_s3_object" "plugins_folder" {
  bucket  = aws_s3_bucket.mwaa.id
  key     = "plugins/"
  content = ""
}

resource "aws_s3_object" "requirements_folder" {
  bucket  = aws_s3_bucket.mwaa.id
  key     = "requirements/"
  content = ""
}

# -------------------------------------------------------
# IAM Role — MWAA Execution Role
# -------------------------------------------------------
data "aws_iam_policy_document" "mwaa_assume_role" {
  statement {
    sid     = "MWAATrustPolicy"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "mwaa_execution" {
  name               = "${var.project_name}-mwaa-execution-role"
  assume_role_policy = data.aws_iam_policy_document.mwaa_assume_role.json

  tags = merge(var.tags, {
    Name = "${var.project_name}-mwaa-execution-role"
  })
}

# -------------------------------------------------------
# IAM Policy — MWAA core permissions
# Follows the AWS-recommended least-privilege policy for MWAA.
# -------------------------------------------------------
data "aws_iam_policy_document" "mwaa_execution" {
  # Allow publishing DAG and task logs to CloudWatch
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:GetLogEvents",
      "logs:GetLogRecord",
      "logs:GetLogDelivery",
      "logs:ListLogDeliveries",
      "logs:CreateLogDelivery",
      "logs:PutRetentionPolicy",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:FilterLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${var.project_name}-*",
    ]
  }

  # Allow MWAA to manage its own SQS queues (used internally for task routing)
  statement {
    sid    = "SQS"
    effect = "Allow"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:ReceiveMessage",
      "sqs:SendMessage",
    ]
    resources = ["arn:aws:sqs:${var.aws_region}:*:airflow-celery-*"]
  }

  # Read/write the MWAA S3 bucket (DAGs, plugins, requirements, logs)
  statement {
    sid    = "S3MWAABucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:GetEncryptionConfiguration",
    ]
    resources = [
      aws_s3_bucket.mwaa.arn,
      "${aws_s3_bucket.mwaa.arn}/*",
    ]
  }

  # Allow MWAA to call itself (web server ↔ scheduler IPC)
  statement {
    sid    = "MWAAEnvironmentAccess"
    effect = "Allow"
    actions = [
      "airflow:PublishMetrics",
    ]
    resources = [
      "arn:aws:airflow:${var.aws_region}:${data.aws_caller_identity.current.account_id}:environment/${var.project_name}-mwaa",
    ]
  }

  # Allow MWAA to use a KMS key for encryption (uses AWS-managed key by default)
  statement {
    sid    = "KMSDecryptEncrypt"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:GenerateDataKey*",
      "kms:Encrypt",
    ]
    not_resources = ["arn:aws:kms:*:${data.aws_caller_identity.current.account_id}:key/*"]
    condition {
      test     = "StringLike"
      variable = "kms:ViaService"
      values   = ["sqs.${var.aws_region}.amazonaws.com", "s3.${var.aws_region}.amazonaws.com"]
    }
  }

  # Allow MWAA to describe EC2 VPC / network resources
  statement {
    sid    = "EC2DescribeNetworking"
    effect = "Allow"
    actions = [
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeVpcs",
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeVpcEndpoints",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "mwaa_execution" {
  name        = "${var.project_name}-mwaa-execution-policy"
  description = "Execution policy for the MWAA environment"
  policy      = data.aws_iam_policy_document.mwaa_execution.json

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "mwaa_execution" {
  role       = aws_iam_role.mwaa_execution.name
  policy_arn = aws_iam_policy.mwaa_execution.arn
}

# -------------------------------------------------------
# MWAA Environment
# -------------------------------------------------------
resource "aws_mwaa_environment" "main" {
  name              = "${var.project_name}-mwaa"
  airflow_version   = var.airflow_version
  environment_class = var.environment_class

  # DAGs live at s3://<bucket>/dags/
  source_bucket_arn    = aws_s3_bucket.mwaa.arn
  dag_s3_path          = "dags/"
  plugins_s3_path      = "plugins/"
  requirements_s3_path = "requirements/requirements.txt"

  execution_role_arn = aws_iam_role.mwaa_execution.arn

  # Public web server — accessible from the public internet
  webserver_access_mode = "PUBLIC_ONLY"

  min_workers = var.min_workers
  max_workers = var.max_workers
  schedulers  = var.schedulers

  network_configuration {
    # MWAA requires private subnets across at least 2 AZs
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.mwaa_security_group_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "WARNING"
    }
    scheduler_logs {
      enabled   = true
      log_level = "WARNING"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "WARNING"
    }
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-mwaa"
  })

  depends_on = [
    aws_s3_bucket_versioning.mwaa,
    aws_s3_bucket_public_access_block.mwaa,
    aws_iam_role_policy_attachment.mwaa_execution,
  ]
}

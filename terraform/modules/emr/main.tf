# -------------------------------------------------------
# IAM Role — EMR Serverless Job Execution Role
# -------------------------------------------------------
data "aws_iam_policy_document" "emr_assume_role" {
  statement {
    sid     = "EMRServerlessTrustPolicy"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_execution" {
  name               = "${var.project_name}-emr-execution-role"
  assume_role_policy = data.aws_iam_policy_document.emr_assume_role.json

  tags = merge(var.tags, {
    Name = "${var.project_name}-emr-execution-role"
  })
}

# -------------------------------------------------------
# IAM Policy — S3 Read/Write
# -------------------------------------------------------
data "aws_iam_policy_document" "emr_s3" {
  statement {
    sid    = "S3BucketAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      var.s3_bucket_arn,
      "${var.s3_bucket_arn}/*",
    ]
  }

  # Allow reading from EMR-managed log / staging buckets in the same region
  statement {
    sid    = "S3EmrDefaultBuckets"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      "arn:aws:s3:::*.elasticmapreduce",
      "arn:aws:s3:::*.elasticmapreduce/*",
    ]
  }
}

resource "aws_iam_policy" "emr_s3" {
  name        = "${var.project_name}-emr-s3-policy"
  description = "Allow EMR Serverless jobs to read/write the project S3 bucket"
  policy      = data.aws_iam_policy_document.emr_s3.json

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_s3" {
  role       = aws_iam_role.emr_execution.name
  policy_arn = aws_iam_policy.emr_s3.arn
}

# -------------------------------------------------------
# IAM Policy — MSK Serverless Read (Kafka client)
# -------------------------------------------------------
data "aws_iam_policy_document" "emr_msk" {
  statement {
    sid    = "MSKServerlessConnect"
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:ReadData",
      "kafka-cluster:WriteData",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:AlterGroup",
    ]
    resources = [
      var.msk_cluster_arn,
      "${var.msk_cluster_arn}/*",
    ]
  }

  # Allow EMR to describe MSK clusters to resolve bootstrap brokers
  statement {
    sid    = "MSKDescribe"
    effect = "Allow"
    actions = [
      "kafka:GetBootstrapBrokers",
      "kafka:DescribeClusterV2",
      "kafka:ListClusters",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "emr_msk" {
  name        = "${var.project_name}-emr-msk-policy"
  description = "Allow EMR Serverless Spark jobs to connect to the Serverless MSK cluster"
  policy      = data.aws_iam_policy_document.emr_msk.json

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_msk" {
  role       = aws_iam_role.emr_execution.name
  policy_arn = aws_iam_policy.emr_msk.arn
}

# -------------------------------------------------------
# IAM Policy — Glue Data Catalog (Spark SQL / Iceberg)
# -------------------------------------------------------
data "aws_iam_policy_document" "emr_glue" {
  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "emr_glue" {
  name        = "${var.project_name}-emr-glue-policy"
  description = "Allow EMR Serverless Spark jobs to use the Glue Data Catalog"
  policy      = data.aws_iam_policy_document.emr_glue.json

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_glue" {
  role       = aws_iam_role.emr_execution.name
  policy_arn = aws_iam_policy.emr_glue.arn
}

# -------------------------------------------------------
# IAM Policy — CloudWatch Logs (job logs)
# -------------------------------------------------------
data "aws_iam_policy_document" "emr_logs" {
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:*:log-group:/emr-serverless/*",
    ]
  }
}

resource "aws_iam_policy" "emr_logs" {
  name        = "${var.project_name}-emr-logs-policy"
  description = "Allow EMR Serverless jobs to write logs to CloudWatch"
  policy      = data.aws_iam_policy_document.emr_logs.json

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_logs" {
  role       = aws_iam_role.emr_execution.name
  policy_arn = aws_iam_policy.emr_logs.arn
}

# -------------------------------------------------------
# EMR Serverless Application — Spark
# -------------------------------------------------------
resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-spark"
  type          = "spark"
  release_label = var.release_label
  architecture  = "X86_64"

  # initial_capacity = 0 — no pre-provisioned workers, no idle cost.
  # Workers are allocated on-demand when a job run is submitted.
  # Omitting the initial_capacity block entirely achieves capacity = 0.

  maximum_capacity {
    cpu    = "200 vCPU"
    memory = "200 GB"
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-spark"
  })
}

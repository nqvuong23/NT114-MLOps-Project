# ------- Instace profice for EC2 -------
resource "aws_iam_role" "ec2_instance_role" {
  name = "${var.project_name}-ec2-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.tags, {
    Name = "${var.project_name}-ec2-instance-role"
  })
}

resource "aws_iam_role_policy_attachment" "ec2_ssm_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
  role       = aws_iam_role.ec2_instance_role.name
}

resource "aws_iam_role_policy_attachment" "ec2_cloudwatch_policy" {
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
  role       = aws_iam_role.ec2_instance_role.name
}

resource "aws_iam_policy" "ec2_s3_policy" {
  name = "${var.project_name}-ec2-s3-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectVersion",
        ]
        Resource = [
          "${var.s3_bucket_arn}/*"
        ]
      },
      {
        Sid    = "S3BucketAccess"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          var.s3_bucket_arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ec2_s3_access" {
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
  role       = aws_iam_policy.ec2_s3_policy.arn
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.project_name}-ec2-instance-profile"
  role = aws_iam_role.ec2_instance_role.name

  tags = merge(var.tags, {
    Name = "${var.project_name}-ec2-instance-profile"
  })
}

# ------- EC2 Instance -------
resource "aws_instance" "main" {
  ami                    = var.ec2_ami_id
  instance_type          = var.ec2_instance_type
  subnet_id              = var.vpc_subnet_id
  vpc_security_group_ids = [var.security_group_id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  root_block_device {
    volume_type           = var.ec2_volume_type
    volume_size           = var.ec2_volume_size
    delete_on_termination = true
    encrypted             = true

    tags = merge(var.tags, {
      Name = "${var.project_name}-data-processing-root-volume"
    })
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  monitoring = var.ec2_enable_detailed_monitoring

  tags = merge(var.tags, {
    Name = "${var.project_name}-data-processing"
    Role = "data-processing"
  })
}


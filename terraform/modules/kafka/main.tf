# ------- Security Group for Serverless MSK Cluster -------
resource "aws_security_group" "msk_serverless" {
  name        = "${var.project_name}-msk-serverless-sg"
  description = "Security group for Serverless MSK cluster"
  vpc_id      = var.vpc_id

  ingress {
    description = "Kafka plaintext (clients inside VPC)"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  ingress {
    description = "Kafka TLS (clients inside VPC)"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  ingress {
    description = "Kafka IAM / SASL_SSL (serverless endpoint)"
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-msk-serverless-sg"
  })
}

# ------- Serverless MSK Cluster -------
resource "aws_msk_serverless_cluster" "main" {
  cluster_name = "${var.project_name}-msk-serverless"

  vpc_config {
    # Public subnet in ap-southeast-1a (index 0)
    subnet_ids = [
      var.public_subnet_ids[0],
      var.public_subnet_ids[1],
    ]
    security_group_ids = [aws_security_group.msk_serverless.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-msk-serverless"
  })
}

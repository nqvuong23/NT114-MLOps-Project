# ------- Serverless MSK Cluster -------
resource "aws_msk_serverless_cluster" "main" {
  cluster_name = "${var.project_name}-msk-serverless"

  vpc_config {
    subnet_ids = var.public_subnet_ids
    security_group_ids = var.security_group_id
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

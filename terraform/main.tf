# ------- Local Values -------
locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    CreatedAt   = formatdate("YYYY-MM-DD", timestamp())
  }
}

# ------- Module: Networking -------
module "networking" {
  source = "./modules/networking"

  project_name         = var.project_name
  aws_region           = var.aws_region
  vpc_cidr             = var.vpc_cidr
  public_subnet_cidrs  = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
  availability_zones   = var.availability_zones
  tags                 = local.common_tags
}

# ------- Module: Storage -------
module "storage" {
  source = "./modules/storage"

  s3_bucket_name       = var.s3_bucket_name
  s3_versioning_status = var.s3_versioning_status
  s3_prefix_list       = var.s3_prefix_list
  project_name         = var.project_name
  tags                 = local.common_tags
}

# ------- Module: Computing -------
module "computing" {
  source = "./modules/computing"

  project_name                   = var.project_name
  aws_region                     = var.aws_region
  ec2_ami_id                     = var.ec2_ami_id
  ec2_instance_type              = var.ec2_instance_type
  ec2_volume_type                = var.ec2_volume_type
  ec2_volume_size                = var.ec2_volume_size
  ec2_enable_detailed_monitoring = var.ec2_enable_detailed_monitoring
  vpc_subnet_id                  = module.networking.public_subnet_ids
  security_group_id              = module.networking.data_processing_security_group_id
  s3_bucket_arn                  = module.storage.s3_bucket_arn
  tags                           = local.common_tags

  depends_on = [module.networking]
}

# ------- Module: Kafka (Serverless MSK) -------
module "kafka" {
  source = "./modules/kafka"

  project_name      = var.project_name
  vpc_id            = module.networking.vpc_id
  public_subnet_ids = module.networking.public_subnet_ids
  tags              = local.common_tags

  depends_on = [module.networking]
}

# ------- Module: EMR Serverless -------
module "emr" {
  source = "./modules/emr"

  project_name    = var.project_name
  aws_region      = var.aws_region
  release_label   = var.emr_release_label
  s3_bucket_arn   = module.storage.s3_bucket_arn
  msk_cluster_arn = module.kafka.msk_serverless_cluster_arn
  tags            = local.common_tags
}

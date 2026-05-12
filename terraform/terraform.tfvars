project_name = "nt114-mlops-project"
aws_region   = "ap-southeast-1"
environment  = "dev"

# ------- Networking Module -------
vpc_cidr = "10.0.0.0/16"
public_subnet_cidrs = [
  "10.0.1.0/24", # ap-southeast-1a
  "10.0.2.0/24", # ap-southeast-1b
]
private_subnet_cidrs = [
  "10.0.11.0/24", # ap-southeast-1a
  "10.0.12.0/24", # ap-southeast-1b
]
availability_zones = [
  "ap-southeast-1a",
  "ap-southeast-1b",
]

# ------- Storage Module -------
s3_bucket_name       = "nt114-mlops-data-bucket"
s3_versioning_status = "Enabled"
s3_prefix_list = [
  "raw-data/",
  "processed-data/",
  "feature-store/",
  "mlflow-artifacts/"
]

# ------- Computing Module -------
ec2_ami_id                     = "ami-0a56f8447277affd8"
ec2_instance_type              = "t3.large"
ec2_volume_type                = "gp3"
ec2_volume_size                = 30
ec2_enable_detailed_monitoring = false

# ------- EMR Serverless Module ------- 
emr_release_label = "emr-7.13.0"

# ------- MWAA Module -------
mwaa_airflow_version   = "2.10.3"
mwaa_environment_class = "mw1.small"
mwaa_min_workers       = 1
mwaa_max_workers       = 3
mwaa_schedulers        = 2

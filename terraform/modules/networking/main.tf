# ------- VPC -------
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-vpc"
  })
}

# ------- Internet Gateway -------
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(var.tags, {
    Name = "${var.project_name}-igw"
  })
}

# ------- Public Subnets -------
resource "aws_subnet" "public" {
  count = length(var.public_subnet_cidrs)

  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-public-subnet-${count.index + 1}"
  })
}

# ------- Private Subnets -------
resource "aws_subnet" "private" {
  count = length(var.private_subnet_cidrs)

  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = var.availability_zones[count.index]

  tags = merge(var.tags, {
    Name = "${var.project_name}-private-subnet-${count.index + 1}"
  })
}

# ------- Elastic IP cho NAT Gateway -------
resource "aws_eip" "nat" {
  domain = "vpc"

  tags = merge(var.tags, {
    Name = "${var.project_name}-nat-eip"
  })

  depends_on = [aws_internet_gateway.main]
}

# ------- NAT Gateway -------
resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  tags = merge(var.tags, {
    Name = "${var.project_name}-nat-gw"
  })

  depends_on = [aws_internet_gateway.main]
}

# ------- Route Table: Public -------
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-public-rt"
  })
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# ------- Route Table: Private -------
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-private-rt"
  })
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# ------- VPC Endpoint for S3 -------
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.main.id
  service_name = "com.amazonaws.${var.aws_region}.s3"

  vpc_endpoint_type = "Gateway"

  tags = merge(var.tags, {
    Name = "${var.project_name}-s3-gateway-endpoint"
  })
}

resource "aws_vpc_endpoint_route_table_association" "s3" {
  route_table_id  = aws_route_table.public.id
  vpc_endpoint_id = aws_vpc_endpoint.s3.id
}

# ------- Security Group -------
resource "aws_security_group" "data_processing" {
  name   = "${var.project_name}-data-processing-sg"
  vpc_id = aws_vpc.main.id

  ingress {
    description = "Airflow UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-data-processing-sg"
  })
}

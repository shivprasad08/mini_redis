provider "aws" {
  region = var.aws_region
}

# Fetch availability zones in region (filtered to available)
data "aws_availability_zones" "available" {
  state = "available"

  # Ensure at least 3 AZs are available
  lifecycle {
    postcondition {
      condition     = length(self.names) >= 3
      error_message = "The selected AWS region must have at least 3 available Availability Zones."
    }
  }
}

# Fetch latest Ubuntu 24.04 LTS AMI (comes with native Python 3.12 matching project requirements >=3.11)
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Key pair for admin SSH (if provided)
resource "aws_key_pair" "admin" {
  count      = var.ssh_public_key != "" ? 1 : 0
  key_name   = "${var.cluster_name}-admin-key"
  public_key = var.ssh_public_key
}

# VPC
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name    = "${var.cluster_name}-vpc"
    Cluster = var.cluster_name
  }
}

# Internet Gateway
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name    = "${var.cluster_name}-igw"
    Cluster = var.cluster_name
  }
}

# Route table for public subnets
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name    = "${var.cluster_name}-public-rt"
    Cluster = var.cluster_name
  }
}

# 3 Subnets across 3 distinct Availability Zones
resource "aws_subnet" "subnets" {
  count                   = 3
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.subnet_cidrs[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name             = "${var.cluster_name}-subnet-${count.index + 1}"
    AvailabilityZone = data.aws_availability_zones.available.names[count.index]
    Cluster          = var.cluster_name
  }
}

# Associate route table with each subnet
resource "aws_route_table_association" "subnets" {
  count          = 3
  subnet_id      = aws_subnet.subnets[count.index].id
  route_table_id = aws_route_table.public.id
}

# Security Group with Least-Privilege Rules
resource "aws_security_group" "cluster" {
  name        = "${var.cluster_name}-sg"
  description = "Security group for mini-redis cluster nodes"
  vpc_id      = aws_vpc.main.id

  # Internal gRPC replication, election, health, time-sync, and mutex communication
  ingress {
    description = "Internal gRPC cluster communication (Replication, Election, Health, Mutex, TimeSync)"
    from_port   = 6381
    to_port     = 6381
    protocol    = "tcp"
    self        = true
  }

  # Internal Redis TCP traffic between nodes if needed
  ingress {
    description = "Internal Redis TCP cluster communication"
    from_port   = 6380
    to_port     = 6380
    protocol    = "tcp"
    self        = true
  }

  # Allow administrative SSH access strictly from specified admin CIDR
  ingress {
    description = "Administrative SSH access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  # Allow external Redis client traffic on port 6380 only from authorized client/admin CIDR
  ingress {
    description = "External client Redis TCP traffic"
    from_port   = 6380
    to_port     = 6380
    protocol    = "tcp"
    cidr_blocks = [var.client_cidr != "" ? var.client_cidr : var.admin_cidr]
  }

  # Outbound internet access for apt packages and pip dependencies during bootstrap
  egress {
    description = "Allow outbound traffic for OS updates and package installation"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.cluster_name}-sg"
    Cluster = var.cluster_name
  }
}

# Node configuration definitions (node-1 is initial primary, node-2 & node-3 are replicas)
locals {
  nodes = [
    {
      name         = "node-1"
      role         = "primary"
      private_ip   = var.node_private_ips[0]
      subnet_index = 0
      replica_of   = "--replica-of ${var.node_private_ips[1]}:6381 --replica-of ${var.node_private_ips[2]}:6381"
      primary_addr = ""
      primary_id   = ""
      peers        = "--peer node-2,${var.node_private_ips[1]}:6381,${var.node_private_ips[1]}:6380 --peer node-3,${var.node_private_ips[2]}:6381,${var.node_private_ips[2]}:6380"
    },
    {
      name         = "node-2"
      role         = "replica"
      private_ip   = var.node_private_ips[1]
      subnet_index = 1
      replica_of   = ""
      primary_addr = "${var.node_private_ips[0]}:6381"
      primary_id   = "node-1"
      peers        = "--peer node-1,${var.node_private_ips[0]}:6381,${var.node_private_ips[0]}:6380 --peer node-3,${var.node_private_ips[2]}:6381,${var.node_private_ips[2]}:6380"
    },
    {
      name         = "node-3"
      role         = "replica"
      private_ip   = var.node_private_ips[2]
      subnet_index = 2
      replica_of   = ""
      primary_addr = "${var.node_private_ips[0]}:6381"
      primary_id   = "node-1"
      peers        = "--peer node-1,${var.node_private_ips[0]}:6381,${var.node_private_ips[0]}:6380 --peer node-2,${var.node_private_ips[1]}:6381,${var.node_private_ips[1]}:6380"
    }
  ]
}

# 3 EC2 Instances across 3 AZs
resource "aws_instance" "nodes" {
  count                  = 3
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.subnets[local.nodes[count.index].subnet_index].id
  private_ip             = local.nodes[count.index].private_ip
  vpc_security_group_ids = [aws_security_group.cluster.id]
  key_name               = var.ssh_public_key != "" ? aws_key_pair.admin[0].key_name : null

  user_data = templatefile("${path.module}/user_data.sh", {
    NODE_ID         = local.nodes[count.index].name
    ROLE            = local.nodes[count.index].role
    PRIMARY_ADDRESS = local.nodes[count.index].primary_addr
    PRIMARY_NODE_ID = local.nodes[count.index].primary_id
    REPLICA_OF_ARGS = local.nodes[count.index].replica_of
    PEER_ARGS       = local.nodes[count.index].peers
    WRITE_MODE      = var.write_mode
    MUTEX_ALGO      = var.mutex_algo
    ELECTION_ALGO   = var.election_algo
  })

  tags = {
    Name             = "${var.cluster_name}-${local.nodes[count.index].name}"
    Role             = local.nodes[count.index].role
    AvailabilityZone = data.aws_availability_zones.available.names[count.index]
    Cluster          = var.cluster_name
  }
}

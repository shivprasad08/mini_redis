variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Unique identifier name for the mini-redis cluster"
  type        = string
  default     = "mini-redis"
}

variable "vpc_cidr" {
  description = "CIDR block for the cluster VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidrs" {
  description = "CIDR blocks for the 3 subnets in 3 distinct availability zones"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "instance_type" {
  description = "EC2 instance type for cluster nodes"
  type        = string
  default     = "t3.small"
}

variable "admin_cidr" {
  description = "CIDR block authorized for administrative SSH access (e.g. your external IP /32). Must not be 0.0.0.0/0."
  type        = string

  validation {
    condition     = var.admin_cidr != "0.0.0.0/0" && can(cidrhost(var.admin_cidr, 0))
    error_message = "admin_cidr must be a valid CIDR block and must NOT be open to the whole internet (0.0.0.0/0)."
  }
}

variable "client_cidr" {
  description = "CIDR block authorized for Redis client TCP port 6380 access. Defaults to admin_cidr."
  type        = string
  default     = ""
}

variable "ssh_public_key" {
  description = "Optional SSH public key string to install onto the instances"
  type        = string
  default     = ""
}

variable "node_private_ips" {
  description = "Static private IPs assigned to the 3 cluster nodes"
  type        = list(string)
  default     = ["10.0.1.10", "10.0.2.10", "10.0.3.10"]
}

variable "election_algo" {
  description = "Leader election algorithm (bully or ring)"
  type        = string
  default     = "bully"

  validation {
    condition     = contains(["bully", "ring"], var.election_algo)
    error_message = "election_algo must be either 'bully' or 'ring'."
  }
}

variable "mutex_algo" {
  description = "Distributed mutual exclusion algorithm (ricart-agrawala or centralized)"
  type        = string
  default     = "ricart-agrawala"

  validation {
    condition     = contains(["ricart-agrawala", "centralized"], var.mutex_algo)
    error_message = "mutex_algo must be either 'ricart-agrawala' or 'centralized'."
  }
}

variable "write_mode" {
  description = "Replication write mode (async or quorum)"
  type        = string
  default     = "async"

  validation {
    condition     = contains(["async", "quorum"], var.write_mode)
    error_message = "write_mode must be either 'async' or 'quorum'."
  }
}

output "vpc_id" {
  description = "The ID of the VPC"
  value       = aws_vpc.main.id
}

output "subnets" {
  description = "Details of the 3 subnets across Availability Zones"
  value = [
    for s in aws_subnet.subnets : {
      id                = s.id
      availability_zone = s.availability_zone
      cidr_block        = s.cidr_block
    }
  ]
}

output "nodes" {
  description = "Cluster node connection and deployment details"
  value = [
    for i in range(3) : {
      node_id           = local.nodes[i].name
      role              = local.nodes[i].role
      instance_id       = aws_instance.nodes[i].id
      availability_zone = aws_instance.nodes[i].availability_zone
      private_ip        = aws_instance.nodes[i].private_ip
      public_ip         = aws_instance.nodes[i].public_ip
      redis_endpoint    = "${aws_instance.nodes[i].public_ip}:6380"
    }
  ]
}

output "primary_redis_endpoint" {
  description = "Public connection endpoint for the initial primary node"
  value       = "${aws_instance.nodes[0].public_ip}:6380"
}

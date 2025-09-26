variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
}

variable "key_name" {
  description = "Key pair name to SSH into the EC2 instance"
  type        = string
}

variable "vpc_name" {
  description = "Name of the existing VPC"
  type        = string
}

variable "subnet_name" {
  description = "Name of the existing VPN subnet"
  type        = string
}

variable "security_group_name" {
  description = "Name of the existing security group"
  type        = string
}

variable "awc_cli_profile" {
  type = string
}

variable "AWS_instance_type" {
  type = string
}

variable "EC2_Name" {
  type = string
}

variable "script_name" {
  type = string
}
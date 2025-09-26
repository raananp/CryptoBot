provider "aws" {
  region = var.aws_region
  profile = var.awc_cli_profile
}


#
# EC2 Instance
resource "aws_instance" "my_vpn" {
  ami           = data.aws_ami.amazon_linux.id
  instance_type = var.AWS_instance_type
  subnet_id     = data.aws_subnet.vpn_subnet.id
  key_name      = var.key_name

  tags = {
    Name = var.EC2_Name
  }
  user_data = file(var.script_name)

  vpc_security_group_ids = [data.aws_security_group.twingate.id]
}

# Output the EC2 public IP
output "instance_public_ip" {
  value = aws_instance.my_vpn.public_ip
}

# Output the key_name for confirmation
output "key_name" {
  value = var.key_name
}
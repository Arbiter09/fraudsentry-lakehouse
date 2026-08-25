# MSK has no free tier -- this is gated behind var.enable_msk (default
# false). Provision only for a short demo session:
#
#   terraform apply -var enable_msk=true -var bronze_bucket_name=<...>
#   ... run producer.py against the bootstrap brokers, take screenshots ...
#   terraform destroy -target=aws_msk_cluster.this -var enable_msk=true -var bronze_bucket_name=<...>
#
# kafka.t3.small is the smallest broker type; 2 brokers across 2 AZs is
# the minimum for a usable demo (single-broker MSK clusters are
# unsupported by the service).

resource "aws_vpc" "msk" {
  count      = var.enable_msk ? 1 : 0
  cidr_block = "10.42.0.0/16"

  tags = { Name = "${var.project_name}-msk-vpc" }
}

resource "aws_subnet" "msk" {
  count             = var.enable_msk ? 2 : 0
  vpc_id            = aws_vpc.msk[0].id
  cidr_block        = "10.42.${count.index}.0/24"
  availability_zone = data.aws_availability_zones.available[0].names[count.index]

  tags = { Name = "${var.project_name}-msk-subnet-${count.index}" }
}

data "aws_availability_zones" "available" {
  count = var.enable_msk ? 1 : 0
  state = "available"
}

resource "aws_security_group" "msk" {
  count  = var.enable_msk ? 1 : 0
  name   = "${var.project_name}-msk-sg"
  vpc_id = aws_vpc.msk[0].id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.42.0.0/16"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_cluster" "this" {
  count                  = var.enable_msk ? 1 : 0
  cluster_name           = "${var.project_name}-cluster"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = aws_subnet.msk[*].id
    security_groups = [aws_security_group.msk[0].id]

    storage_info {
      ebs_storage_info {
        volume_size = 20 # GB per broker, smallest practical size
      }
    }
  }

  tags = { Project = var.project_name }
}

output "msk_bootstrap_brokers" {
  value = var.enable_msk ? aws_msk_cluster.this[0].bootstrap_brokers : null
}

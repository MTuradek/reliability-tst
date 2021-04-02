provider "aws" {
  region = "us-east-1"
  profile = "swarthy"
}

resource "aws_s3_bucket" "legacy-s3-test-2021" {
  bucket = "legacy-s3-test-2021"
}

resource "aws_s3_bucket_public_access_block" "legacy-s3-test-2021" {
  bucket = aws_s3_bucket.legacy-s3-test-2021.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "production-s3-test-2021" {
  bucket = "production-s3-test-2021"
  acl = "private"
}

resource "aws_s3_bucket_public_access_block" "production-s3-test-2021" {
  bucket = aws_s3_bucket.production-s3-test-2021.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}

resource "aws_security_group" "rds_security_group" {
  name = "rds_proddatabase"
  description = "RDS Security Group"

  ingress {
    from_port = 5432
    to_port = 5432
    protocol = "tcp"
    cidr_blocks = [
      "31.135.174.222/32"]
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [
      "0.0.0.0/0"]
  }
}

resource "aws_db_instance" "db-proddatabase" {
  engine = "postgres"
  engine_version = "12.5"
  identifier = "rds-proddatabase"
  instance_class = "db.t2.micro"
  allocated_storage = "20"
  name = "proddatabase"
  username = "postgres_db_admin"
  password = "postgres_db_productiondatabase"
  publicly_accessible = "true"
  vpc_security_group_ids = [
    aws_security_group.rds_security_group.id]
  skip_final_snapshot = true
}
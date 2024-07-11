# provider.tf
provider "aws" {
  region = "us-east-1"
  access_key = var.aws_acess_key
  secret_key = var.aws_acess_secret_key
}

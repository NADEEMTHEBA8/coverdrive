# All inputs to the Coverdrive AWS module.
# Sensible defaults for portfolio use; override via terraform.tfvars or -var.

variable "project_name" {
  description = "Short name used as a prefix for every resource."
  type        = string
  default     = "coverdrive"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.project_name))
    error_message = "project_name must be 3–21 chars, lowercase, alphanumeric or hyphen, starting with a letter."
  }
}

variable "environment" {
  description = "Deployment environment: dev | staging | prod. Drives multi-AZ, backup retention, and deletion protection."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "ap-south-1" # Mumbai — closest to Bengaluru for the portfolio narrative
}

variable "owner" {
  description = "Tag applied to all resources for cost attribution."
  type        = string
  default     = "nadeem.theba"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC. The module carves /24 subnets out of this."
  type        = string
  default     = "10.42.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vpc_cidr))
    error_message = "vpc_cidr must be a valid IPv4 CIDR block."
  }
}

variable "db_instance_class" {
  description = "RDS instance class for Airflow metadata DB. db.t4g.micro is free-tier eligible."
  type        = string
  default     = "db.t4g.micro"
}

variable "db_username" {
  description = "Master username for the Airflow metadata DB."
  type        = string
  default     = "airflow"

  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9_]{2,15}$", var.db_username))
    error_message = "db_username must start with a letter and be 3–16 chars."
  }
}

variable "db_password" {
  description = "Master password for the Airflow metadata DB. NEVER commit this. Inject via AWS SSM Parameter Store or `terraform apply -var`."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.db_password) >= 16
    error_message = "db_password must be at least 16 characters."
  }
}

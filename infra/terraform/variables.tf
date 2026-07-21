# All inputs to the Coverdrive AWS module.
# Sensible defaults for dev environments; override via terraform.tfvars or -var.

variable "environment" {
  description = "Deployment environment: dev | staging | prod"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "project_name" {
  description = "Project name prefix used for all AWS resource naming"
  type        = string
  default     = "coverdrive"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.project_name))
    error_message = "project_name must be 3–21 chars, lowercase, alphanumeric or hyphen, starting with a letter."
  }
}

variable "aws_region" {
  description = "AWS region for all infrastructure resources"
  type        = string
  default     = "ap-south-1" # Primary AWS region
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
variable "alert_email" {
  description = "Email address to subscribe to the alerts SNS topic. Leave empty to skip (alarms fire but deliver nowhere)."
  type        = string
  default     = ""
}

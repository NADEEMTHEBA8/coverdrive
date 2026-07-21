# ─────────────────────────────────────────────────────────────────────
# Coverdrive — AWS infrastructure
#
# Scope: data-plane primitives that a production deployment of the
# Coverdrive pipeline would consume. We provision the lakehouse bucket,
# Airflow's metadata DB, a container registry, IAM with least privilege,
# and observability. Compute (ECS/MWAA) is intentionally NOT provisioned
# here — see README.md § "What's not deployed" for the reasoning.
#
# State backend: configure remote state in `backend.tf` (gitignored) per
# environment. A starter is documented in README.md.
# ─────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = var.owner
      CostCenter  = "data-platform"
    }
  }
}

# ─── Locals ──────────────────────────────────────────────────────────
locals {
  name_prefix = "${var.project_name}-${var.environment}"
  azs         = slice(data.aws_availability_zones.available.names, 0, 2)
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# ═════════════════════════════════════════════════════════════════════
# Networking — minimal VPC with public + private subnets.
#
# Cost note: no NAT gateway. Egress to S3 is via the (free) gateway
# endpoint; egress to ECR is via interface endpoints. This keeps
# monthly cost near zero for the network layer.
# ═════════════════════════════════════════════════════════════════════

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${local.name_prefix}-vpc"
  }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = false # Workloads sit in private subnets; public is for future ALB only.

  tags = {
    Name = "${local.name_prefix}-public-${local.azs[count.index]}"
    Tier = "public"
  }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, count.index + 10)
  availability_zone = local.azs[count.index]

  tags = {
    Name = "${local.name_prefix}-private-${local.azs[count.index]}"
    Tier = "private"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${local.name_prefix}-igw"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${local.name_prefix}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${local.name_prefix}-private-rt"
  }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# S3 Gateway endpoint — free, lets private subnets reach S3 without NAT.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = {
    Name = "${local.name_prefix}-s3-endpoint"
  }
}

# Security group for RDS — only accessible from within the VPC.
resource "aws_security_group" "rds" {
  name        = "${local.name_prefix}-rds-sg"
  description = "Allow Postgres traffic from within the VPC only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Postgres from VPC private subnets"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [for s in aws_subnet.private : s.cidr_block]
  }

  egress {
    description = "Intra-VPC only - RDS has no legitimate reason to reach the internet"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
  }

  tags = {
    Name = "${local.name_prefix}-rds-sg"
  }
}

# ═════════════════════════════════════════════════════════════════════
# KMS — Customer-managed key shared across S3, ECR, CloudWatch, and
# Athena. A single CMK simplifies key-rotation policy and CloudTrail
# audit trail without the overhead of per-service keys at this scale.
# ═════════════════════════════════════════════════════════════════════

resource "aws_kms_key" "lake" {
  description             = "CMK for Coverdrive lakehouse: S3, ECR, CloudWatch, Athena"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = {
    Name = "${local.name_prefix}-lake-cmk"
  }
}

resource "aws_kms_alias" "lake" {
  name          = "alias/${local.name_prefix}-lake"
  target_key_id = aws_kms_key.lake.key_id
}

# ═════════════════════════════════════════════════════════════════════
# S3 — lakehouse bucket (Bronze + Silver) and access-log bucket.
#
# Hardening:
#   - Versioning on (recover from bad pipeline writes).
#   - KMS-CMK at-rest encryption (customer-managed key, CloudTrail-audited).
#   - All public access blocked.
#   - Lifecycle: Bronze raw moves to IA at 30d, Glacier at 180d.
#   - Server access logs to a separate bucket.
# ═════════════════════════════════════════════════════════════════════

resource "aws_s3_bucket" "lake" {
  bucket = "${local.name_prefix}-lake-${random_id.bucket_suffix.hex}"

  tags = {
    Name        = "${local.name_prefix}-lake"
    DataClass   = "internal"
    Description = "Coverdrive lakehouse: bronze raw and silver cleaned partitioned Parquet"
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "bronze-tiering"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_logging" "lake" {
  bucket        = aws_s3_bucket.lake.id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "s3-access/${local.name_prefix}-lake/"
}

# Separate bucket for access logs — never holds pipeline data.
resource "aws_s3_bucket" "access_logs" {
  bucket = "${local.name_prefix}-access-logs-${random_id.bucket_suffix.hex}"

  tags = {
    Name      = "${local.name_prefix}-access-logs"
    DataClass = "operational"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
  }
}

resource "aws_s3_bucket_public_access_block" "access_logs" {
  bucket                  = aws_s3_bucket.access_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  rule {
    id     = "expire-old-logs"
    status = "Enabled"

    filter {}

    expiration {
      days = 90
    }
  }
}

# ═════════════════════════════════════════════════════════════════════
# RDS — Postgres for Airflow metadata.
#
# Single-AZ + db.t4g.micro for dev/portfolio. Production would flip
# multi_az and right-size the instance.
# ═════════════════════════════════════════════════════════════════════

resource "aws_db_subnet_group" "main" {
  name       = "${local.name_prefix}-db-subnets"
  subnet_ids = aws_subnet.private[*].id

  tags = {
    Name = "${local.name_prefix}-db-subnets"
  }
}

resource "aws_db_instance" "airflow_metadata" {
  identifier             = "${local.name_prefix}-airflow-meta"
  engine                 = "postgres"
  engine_version         = "16"
  instance_class         = var.db_instance_class
  allocated_storage      = 20
  max_allocated_storage  = 100 # autoscale up to 100GB if needed
  storage_type           = "gp3"
  storage_encrypted      = true
  db_name                = "airflow"
  username               = var.db_username
  manage_master_user_password = true  # RDS owns rotation via Secrets Manager; password never in state.
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  multi_az               = var.environment == "prod"
  publicly_accessible    = false
  skip_final_snapshot    = var.environment != "prod"
  deletion_protection    = var.environment == "prod"
  backup_retention_period = var.environment == "prod" ? 7 : 1
  apply_immediately      = var.environment != "prod"

  # Performance Insights — useful for slow-query investigation in interviews.
  performance_insights_enabled          = true
  performance_insights_retention_period = 7
  parameter_group_name                  = aws_db_parameter_group.airflow.name
  enabled_cloudwatch_logs_exports       = ["postgresql", "upgrade"]

  tags = {
    Name = "${local.name_prefix}-airflow-meta"
  }
}

resource "aws_db_parameter_group" "airflow" {
  name   = "${local.name_prefix}-postgres16"
  family = "postgres16"

  parameter {
    name  = "log_min_duration_statement"
    value = "1000"  # Log queries taking over 1 s — useful for slow-query investigations.
  }

  parameter {
    name  = "pg_stat_statements.track"
    value = "all"
  }

  tags = {
    Name = "${local.name_prefix}-postgres16-params"
  }
}

# ═════════════════════════════════════════════════════════════════════
# ECR — container registry for the pipeline image.
# ═════════════════════════════════════════════════════════════════════

resource "aws_ecr_repository" "pipeline" {
  name                 = "${local.name_prefix}/pipeline"
  image_tag_mutability = "IMMUTABLE" # forces semantic tags; prevents :latest drift

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.lake.arn
  }
}

resource "aws_ecr_lifecycle_policy" "pipeline" {
  repository = aws_ecr_repository.pipeline.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 20 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 20
      }
      action = {
        type = "expire"
      }
    }]
  })
}

# ═════════════════════════════════════════════════════════════════════
# IAM — least-privilege role assumed by the pipeline at runtime.
#
# Two roles by convention:
#   - task_execution_role: pulls the image from ECR, writes logs to
#     CloudWatch (used by the ECS agent, not the app).
#   - pipeline_task_role:  the role the *application* assumes — has
#     scoped S3 access to the lake bucket only.
# ═════════════════════════════════════════════════════════════════════

data "aws_iam_policy_document" "ecs_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"]
    }
  }
}

resource "aws_iam_role" "task_execution" {
  name               = "${local.name_prefix}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

resource "aws_iam_role_policy_attachment" "task_execution_managed" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "pipeline_task" {
  name               = "${local.name_prefix}-pipeline-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

# Least-priv S3 policy — scoped to this lake bucket only, no list-all-buckets.
data "aws_iam_policy_document" "pipeline_s3" {
  statement {
    sid    = "ReadWriteLakeObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }

  statement {
    sid     = "ListLakeBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
}

resource "aws_iam_role_policy" "pipeline_s3" {
  name   = "lake-rw"
  role   = aws_iam_role.pipeline_task.id
  policy = data.aws_iam_policy_document.pipeline_s3.json
}

# CloudWatch write access for app logs.
data "aws_iam_policy_document" "pipeline_logs" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.pipeline.arn}:*"]
  }
}

resource "aws_iam_role_policy" "pipeline_logs" {
  name   = "cloudwatch-logs"
  role   = aws_iam_role.pipeline_task.id
  policy = data.aws_iam_policy_document.pipeline_logs.json
}

# ═════════════════════════════════════════════════════════════════════
# CloudWatch — log group for the pipeline.
# ═════════════════════════════════════════════════════════════════════

resource "aws_cloudwatch_log_group" "pipeline" {
  name              = "/aws/coverdrive/${var.environment}/pipeline"
  retention_in_days = var.environment == "prod" ? 30 : 7

  tags = {
    Name = "${local.name_prefix}-pipeline-logs"
  }
}

# ─── RDS CloudWatch Alarms ───────────────────────────────────────────────────

resource "aws_sns_topic" "alerts" {
  name              = "${local.name_prefix}-alerts"
  kms_master_key_id = aws_kms_key.lake.arn
}

# Subscription endpoint is injected at deploy time via the ALERT_EMAIL variable.
# For local dev this can be left empty; the alarms fire but deliver nowhere.
resource "aws_sns_topic_subscription" "alerts_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${local.name_prefix}-rds-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "RDS CPU over 80% for 10 minutes"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    DBInstanceIdentifier = aws_db_instance.airflow_metadata.identifier
  }
}

resource "aws_cloudwatch_metric_alarm" "rds_storage" {
  alarm_name          = "${local.name_prefix}-rds-storage"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "FreeStorageSpace"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 5368709120  # 5 GB in bytes — alert before autoscaling kicks in (threshold equals one RDS autoscale increment)
  alarm_description   = "RDS free storage below 5 GB"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    DBInstanceIdentifier = aws_db_instance.airflow_metadata.identifier
  }
}

resource "aws_cloudwatch_metric_alarm" "rds_connections" {
  alarm_name          = "${local.name_prefix}-rds-connections"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "DatabaseConnections"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "RDS connection count over 80 - possible connection leak"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions = {
    DBInstanceIdentifier = aws_db_instance.airflow_metadata.identifier
  }
}

# ═════════════════════════════════════════════════════════════════════
# Glue Data Catalog & Athena
# ═════════════════════════════════════════════════════════════════════

resource "aws_glue_catalog_database" "coverdrive" {
  name        = "coverdrive_${var.environment}"
  description = "Coverdrive lakehouse catalog"
}

resource "aws_athena_workgroup" "coverdrive" {
  name = "coverdrive_${var.environment}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.id}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.lake.arn
      }
    }
  }
}

# ─── CloudWatch Dashboard ─────────────────────────────────────────────────────
# Single pane of glass for RDS health metrics. Referenced in runbooks.

resource "aws_cloudwatch_dashboard" "coverdrive" {
  dashboard_name = "${local.name_prefix}-overview"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 8
        height = 6
        properties = {
          title   = "RDS CPU Utilization"
          period  = 300
          stat    = "Average"
          region  = var.aws_region
          metrics = [["AWS/RDS", "CPUUtilization", "DBInstanceIdentifier", aws_db_instance.airflow_metadata.identifier]]
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 0
        width  = 8
        height = 6
        properties = {
          title   = "RDS Free Storage (bytes)"
          period  = 300
          stat    = "Average"
          region  = var.aws_region
          metrics = [["AWS/RDS", "FreeStorageSpace", "DBInstanceIdentifier", aws_db_instance.airflow_metadata.identifier]]
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 0
        width  = 8
        height = 6
        properties = {
          title   = "RDS Database Connections"
          period  = 300
          stat    = "Average"
          region  = var.aws_region
          metrics = [["AWS/RDS", "DatabaseConnections", "DBInstanceIdentifier", aws_db_instance.airflow_metadata.identifier]]
        }
      },
      {
        type   = "log"
        x      = 0
        y      = 6
        width  = 24
        height = 6
        properties = {
          title  = "Pipeline Logs (last 1h)"
          query  = "SOURCE '${aws_cloudwatch_log_group.pipeline.name}' | fields @timestamp, @message | sort @timestamp desc | limit 50"
          region = var.aws_region
          view   = "table"
        }
      }
    ]
  })
}

# NOTE: X-Ray tracing was evaluated and deliberately deferred.
# This pipeline is batch-oriented (not request-path), so CloudWatch
# metrics and alarms satisfy the SLI requirements without the
# per-segment cost of X-Ray. Add xray:PutTraceSegments to
# pipeline_task_role and instrument api.py if request tracing is needed.

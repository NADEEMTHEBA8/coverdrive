# Coverdrive — AWS infrastructure

Terraform module that provisions the **data-plane primitives** for a
production deployment of Coverdrive.

## What this provisions

| Component | Purpose |
|---|---|
| VPC (2 AZs, public + private subnets, S3 gateway endpoint) | Network isolation; no NAT to keep costs near zero |
| S3 lakehouse bucket | Bronze + Silver Parquet; versioning, SSE-S3, lifecycle (Standard → IA → Glacier), public access blocked, server access logs |
| S3 access-log bucket | Audit trail, 90-day expiration |
| RDS Postgres | Airflow metadata DB; encrypted, private, Performance Insights enabled |
| ECR repository | Immutable-tag image registry for the pipeline container |
| IAM (2 roles, 2 policies) | Least-privilege: task execution + pipeline task with bucket-scoped S3 access |
| CloudWatch log group | Pipeline logs |

## What's **not** deployed (and why)

- **Compute (ECS service / MWAA)** — the running Airflow stack itself.
  This module intentionally stops at the IaC primitives the compute
  layer would consume. Reasoning:
  1. Choice of compute is environment-specific. Dev should run the
     `docker-compose.yml` stack locally. Staging might use ECS Fargate;
     prod might use MWAA. Encoding one of those into the same module
     blurs the boundary between data plane and orchestration plane.
  2. MWAA costs ~$350/month minimum and is not portfolio-budget-friendly.
  3. ECS Task Definition + Service is `terraform`-able in ~80 LoC and
     belongs in a separate `infra/terraform/compute/` module when needed.

- **CDN / public load balancer for the API** — out of scope; the API
  is a portfolio artifact, not a production service.

- **KMS CMK** — bucket uses SSE-S3 (AES-256). A real production setup
  would create a customer-managed key with rotation; the trade-off is
  ~$1/month + key-policy complexity that distracts from the data-engineering
  story. Documented as a known gap.

## Cost estimate (ap-south-1, monthly)

| Resource | Estimated cost (USD) |
|---|---|
| RDS `db.t4g.micro` (free tier eligible for 12mo) | **$0** (or ~$13 outside free tier) |
| S3 storage (~5 GB Parquet + versioning) | **<$0.50** |
| ECR (1 GB image storage) | **<$0.10** |
| CloudWatch Logs (low volume) | **<$0.50** |
| VPC + S3 endpoint | **$0** |
| **Total** | **~$1–14 / month** |

There is **no NAT Gateway** (would be ~$32/mo) — egress to AWS services
goes through the S3 gateway endpoint (free). Private subnets cannot reach
the public internet; pipeline jobs that need that should run in public
subnets with explicit security groups.

## Prerequisites

- Terraform ≥ 1.6
- AWS CLI configured (`aws configure`) with credentials that can create
  VPC, RDS, S3, ECR, IAM, and CloudWatch resources.
- For state management: an S3 bucket + DynamoDB table you've already
  created out-of-band. See [State backend](#state-backend) below.

## Apply

```bash
cd infra/terraform

# 1. Configure inputs
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars

# 2. Provide the DB password via env var (never commit it)
export TF_VAR_db_password="$(openssl rand -base64 24)"

# 3. Plan
terraform init
terraform plan -out=tfplan

# 4. Apply
terraform apply tfplan

# 5. Capture outputs into your local .env
terraform output -raw lake_bucket_name | xargs -I{} echo "COVERDRIVE_S3_BUCKET={}" >> ../../.env
```

## Teardown

```bash
# RDS will fail to delete in prod due to deletion_protection — that's by
# design. For dev/staging:
terraform destroy
```

If `terraform destroy` hangs on the S3 bucket, it's because versioning
left object versions behind. Empty them first:

```bash
aws s3api delete-objects \
  --bucket "$(terraform output -raw lake_bucket_name)" \
  --delete "$(aws s3api list-object-versions \
    --bucket "$(terraform output -raw lake_bucket_name)" \
    --output json \
    --query='{Objects: Versions[].{Key:Key,VersionId:VersionId}}')"
```

## State backend

Local state is fine for the portfolio demo, but a real deployment needs
remote state with locking. Create the backend resources once, then add
a `backend.tf`:

```hcl
terraform {
  backend "s3" {
    bucket         = "my-tf-state-bucket"
    key            = "coverdrive/dev/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "terraform-state-locks"
    encrypt        = true
  }
}
```

`backend.tf` is gitignored to keep state pointers out of the repo.

## Security notes

- All buckets block all public access (`block_public_*` = true).
- RDS is in private subnets only; no public IP.
- Storage is encrypted at rest (SSE-S3) and in transit (RDS forces TLS).
- Pipeline IAM policy is bucket-scoped — no `s3:*` on `Resource: "*"`.
- The DB password is `sensitive` and excluded from plan output.
- ECR tags are immutable — no silent `:latest` overwrites.

## Known gaps (intentional, would-fix-in-prod)

1. No KMS CMK — see "What's not deployed".
2. No VPC Flow Logs — add an `aws_flow_log` if you need network forensics.
3. Single-region — multi-region replication (`aws_s3_bucket_replication_configuration`) is a one-paragraph addition when DR matters.
4. No WAF / Shield — the API isn't internet-facing in this module.
5. No Secrets Manager rotation — `db_password` is static; production would use `aws_secretsmanager_secret_rotation`.

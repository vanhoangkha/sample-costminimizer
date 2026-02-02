# CostMinimizer Docker Deployment

AI-powered AWS cost optimization tool deployed via Docker on EC2.

![Architecture](costminimizer-architecture.png)

## Quick Deploy

```bash
# Deploy infrastructure
aws cloudformation deploy --template-file cloudformation-deploy.yaml \
  --stack-name costminimizer --capabilities CAPABILITY_NAMED_IAM

# Build & push Docker image
./docker-deploy.sh
```

## Architecture

| Component | Resource |
|-----------|----------|
| Compute | EC2 t3.medium + Docker |
| Container Registry | ECR (costminimizer:secure) |
| Storage | S3 (encrypted reports) |
| Scheduling | EventBridge (Weekly Monday 8AM) |
| Access | SSM Session Manager (no SSH) |
| Monitoring | CloudWatch Logs + SNS |
| AI Analysis | Bedrock |

## Security Features

- ✅ Slim base image (0 Critical/High CVEs)
- ✅ Outbound HTTPS only (port 443)
- ✅ S3 encryption (AES-256) + HTTPS enforced
- ✅ IMDSv2 required
- ✅ No SSH keys (SSM access only)
- ✅ ECR image scanning enabled
- ✅ IAM least privilege

## Usage

```bash
# Run Cost Explorer reports
aws ssm send-command --instance-ids <INSTANCE_ID> \
  --document-name "CostMinimizer-RunReports" \
  --parameters 'ReportTypes=--ce'

# Run all reports (CE + Trusted Advisor + Compute Optimizer)
aws ssm send-command --instance-ids <INSTANCE_ID> \
  --document-name "CostMinimizer-RunReports" \
  --parameters 'ReportTypes=--ce --ta --co'

# Download reports
aws s3 sync s3://costminimizer-reports-<ACCOUNT_ID>/ ./reports/
```

## Files

| File | Description |
|------|-------------|
| `Dockerfile.secure` | Hardened Docker image |
| `cloudformation-deploy.yaml` | Full infrastructure as code |
| `docker-deploy.sh` | ECR build & push script |
| `iam-policy.json` | IAM policy template |

## Cost

~$31/month (on-demand) or ~$10/month (Spot Instance)

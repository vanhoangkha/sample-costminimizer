# CostMinimizer Docker Deployment

AI-powered AWS cost optimization tool deployed via Docker on EC2.

## Architecture

![CostMinimizer Architecture](./costminimizer-architecture.png)

### Components

| Layer | Service | Purpose |
|-------|---------|---------|
| **Access** | SSM Session Manager | Secure access (no SSH) |
| **Compute** | EC2 + Docker | Run CostMinimizer container |
| **Registry** | ECR | Store secure Docker image |
| **Schedule** | EventBridge | Weekly automated runs |
| **Data** | Cost Explorer, Trusted Advisor, Compute Optimizer | AWS cost data |
| **AI** | Bedrock | Generate recommendations |
| **Storage** | S3 | Store reports (encrypted) |
| **Monitor** | CloudWatch, SNS | Logs & notifications |

## Quick Deploy

```bash
# 1. Deploy infrastructure
aws cloudformation deploy --template-file cloudformation-deploy.yaml \
  --stack-name costminimizer --capabilities CAPABILITY_NAMED_IAM

# 2. Build & push Docker image
./docker-deploy.sh
```

## Security

| Feature | Status |
|---------|--------|
| Base image CVEs (Critical/High) | ✅ 0 |
| Outbound traffic | ✅ HTTPS only |
| S3 encryption | ✅ AES-256 |
| Instance metadata | ✅ IMDSv2 |
| Access method | ✅ SSM (no SSH) |
| ECR scanning | ✅ Enabled |

## Usage

```bash
# Run reports
aws ssm send-command --instance-ids <INSTANCE_ID> \
  --document-name "CostMinimizer-RunReports" \
  --parameters 'ReportTypes=--ce --ta --co'

# Download reports
aws s3 sync s3://costminimizer-reports-<ACCOUNT_ID>/ ./reports/
```

## Cost

| Resource | Monthly |
|----------|---------|
| EC2 t3.medium | ~$30 |
| S3 + ECR + Logs | ~$1 |
| **Total** | **~$31** |

💡 Use Spot Instance to reduce to ~$10/month

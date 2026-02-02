# CostMinimizer Docker Deployment Summary

## Resources Created

| Resource | Value |
|----------|-------|
| ECR Repository | `067847735476.dkr.ecr.us-east-1.amazonaws.com/costminimizer` |
| EC2 Instance | `i-0c7801d33ae5fc873` |
| IAM Role | `CostMinimizerEC2Role` |
| S3 Bucket | `costminimizer-reports-067847735476` |
| SNS Topic | `costminimizer-notifications` |
| EventBridge Rule | `costminimizer-weekly` (Monday 8AM UTC) |
| SSM Document | `CostMinimizer-RunReports` |

## Security Hardening Applied

- ✅ Slim base image (python:3.12-slim) - 0 Critical/High CVEs
- ✅ Outbound HTTPS only (port 443)
- ✅ S3 encryption (AES-256)
- ✅ S3 HTTPS enforced
- ✅ IMDSv2 required
- ✅ No SSH keys (SSM access only)
- ✅ ECR image scanning enabled

## Usage

```bash
# Run reports manually
aws ssm send-command --instance-ids i-0c7801d33ae5fc873 \
  --document-name "CostMinimizer-RunReports" \
  --parameters 'ReportTypes=--ce --ta --co'

# Download reports
aws s3 sync s3://costminimizer-reports-067847735476/ ./reports/
```

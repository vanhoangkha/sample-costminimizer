#!/bin/bash
# CostMinimizer Docker Deployment Script
set -e

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=${AWS_REGION:-us-east-1}
ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/costminimizer"

echo "=== Building and pushing to ECR ==="
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com
docker build -f Dockerfile.secure -t costminimizer:secure .
docker tag costminimizer:secure $ECR_URI:latest
docker push $ECR_URI:latest
echo "✅ Image pushed to $ECR_URI:latest"

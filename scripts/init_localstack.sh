#!/usr/bin/env bash
# scripts/init_localstack.sh
#
# creates the s3 bucket in localstack on first run.
# run this once after `docker compose up`:
#   bash scripts/init_localstack.sh

set -euo pipefail

# localstack accepts any credentials, but the aws cli still needs *some* set or it
# errors with NoCredentials. the real creds live in .env (read by the containers, not
# this host shell), so default them here to keep the script runnable from any machine.
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

ENDPOINT="http://localhost:4566"
BUCKET="developer-job-market"

echo "⏳  Waiting for LocalStack to be ready..."
until curl -sf "$ENDPOINT/_localstack/health" | grep -q '"s3": "available"'; do
  sleep 2
done

echo "✅  LocalStack is up."

echo "🪣  Creating S3 bucket: $BUCKET"
aws --endpoint-url "$ENDPOINT" \
    --region us-east-1 \
    s3 mb "s3://$BUCKET" 2>/dev/null || echo "   Bucket already exists — skipping."

echo ""
echo "📂  Bucket contents:"
aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET" || echo "   (empty)"

echo ""
echo "Done. Your Datalake bucket is ready at s3://$BUCKET"

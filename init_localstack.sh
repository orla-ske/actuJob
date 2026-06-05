#!/usr/bin/env bash
# scripts/init_localstack.sh
#
# Creates the S3 bucket in LocalStack on first run.
# Run this once after `docker compose up`:
#   bash scripts/init_localstack.sh

set -euo pipefail

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

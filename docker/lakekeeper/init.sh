#!/bin/sh
# =============================================================================
# Lakekeeper first-start setup, idempotent (run by a one-shot curl container):
# bootstrap the server, then create the Iceberg warehouse on Garage if missing.
# Storage: S3-compatible, path-style, no STS (Garage has none), remote signing on:
# Lakekeeper signs its clients' S3 requests, Spark holds no S3 key.
# Env: LAKEKEEPER_URL, ICEBERG_WAREHOUSE, BUCKET, ICEBERG_KEY_PREFIX, S3_ENDPOINT_URL,
#      AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
# =============================================================================
set -eu

code=$(curl -s -o /tmp/out -w "%{http_code}" -X POST "$LAKEKEEPER_URL/management/v1/bootstrap" \
  -H "Content-Type: application/json" -d '{"accept-terms-of-use": true}')
case "$code" in
  204) echo "Lakekeeper bootstrapped" ;;
  400) echo "Lakekeeper already bootstrapped" ;;
  *) echo "Bootstrap failed: HTTP $code $(cat /tmp/out)"; exit 1 ;;
esac

if curl -sf "$LAKEKEEPER_URL/management/v1/warehouse" | grep -q "\"name\":\"$ICEBERG_WAREHOUSE\""; then
  echo "Warehouse $ICEBERG_WAREHOUSE already exists"
  exit 0
fi

code=$(curl -s -o /tmp/out -w "%{http_code}" -X POST "$LAKEKEEPER_URL/management/v1/warehouse" \
  -H "Content-Type: application/json" -d @- <<JSON
{
  "warehouse-name": "$ICEBERG_WAREHOUSE",
  "storage-profile": {
    "type": "s3", "bucket": "$BUCKET", "key-prefix": "$ICEBERG_KEY_PREFIX",
    "region": "$AWS_DEFAULT_REGION", "endpoint": "$S3_ENDPOINT_URL",
    "path-style-access": true, "flavor": "s3-compat",
    "sts-enabled": false, "remote-signing-enabled": true
  },
  "storage-credential": {
    "type": "s3", "credential-type": "access-key",
    "aws-access-key-id": "$AWS_ACCESS_KEY_ID", "aws-secret-access-key": "$AWS_SECRET_ACCESS_KEY"
  }
}
JSON
)
# 201: created (Lakekeeper has written and read a test object on the bucket)
[ "$code" = "201" ] || { echo "Warehouse creation failed: HTTP $code $(cat /tmp/out)"; exit 1; }
echo "Warehouse $ICEBERG_WAREHOUSE created"

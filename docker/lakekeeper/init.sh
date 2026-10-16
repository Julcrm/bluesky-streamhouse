#!/bin/sh
# =============================================================================
# Lakekeeper first-start setup, idempotent (run by a one-shot curl container):
# bootstrap the server, create the Iceberg warehouse on Garage if missing, then the
# `default` namespace (dbt-spark's PyHive runs `USE default` on every connection).
# Storage: S3-compatible, path-style, no STS (Garage has none), no remote signing (since
# 2026-10-16): Spark uses its own S3 key, like the DuckDB branch, instead of one Lakekeeper
# call per S3 request. The storage profile is applied at every start, so an existing
# warehouse follows this file (Lakekeeper re-checks the bucket with a test object).
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

# Storage profile and credential of the warehouse (Lakekeeper's own key: it deletes the
# files of dropped tables, hard delete profile)
storage() {
  cat <<JSON
  "storage-profile": {
    "type": "s3", "bucket": "$BUCKET", "key-prefix": "$ICEBERG_KEY_PREFIX",
    "region": "$AWS_DEFAULT_REGION", "endpoint": "$S3_ENDPOINT_URL",
    "path-style-access": true, "flavor": "s3-compat",
    "sts-enabled": false, "remote-signing-enabled": false
  },
  "storage-credential": {
    "type": "s3", "credential-type": "access-key",
    "aws-access-key-id": "$AWS_ACCESS_KEY_ID", "aws-secret-access-key": "$AWS_SECRET_ACCESS_KEY"
  }
JSON
}

create_warehouse() {
  code=$(printf '{"warehouse-name": "%s",\n%s}' "$ICEBERG_WAREHOUSE" "$(storage)" \
    | curl -s -o /tmp/out -w "%{http_code}" -X POST "$LAKEKEEPER_URL/management/v1/warehouse" \
      -H "Content-Type: application/json" -d @-)
  # 201: created (Lakekeeper has written and read a test object on the bucket)
  [ "$code" = "201" ] || { echo "Warehouse creation failed: HTTP $code $(cat /tmp/out)"; exit 1; }
  echo "Warehouse $ICEBERG_WAREHOUSE created"
}

if curl -sf "$LAKEKEEPER_URL/management/v1/warehouse" | grep -q "\"name\":\"$ICEBERG_WAREHOUSE\""; then
  echo "Warehouse $ICEBERG_WAREHOUSE already exists"
else
  create_warehouse
fi

# Iceberg REST prefix of the warehouse (its id), from the catalog config
prefix=$(curl -sf "$LAKEKEEPER_URL/catalog/v1/config?warehouse=$ICEBERG_WAREHOUSE" \
  | sed -n 's/.*"prefix":"\([^"]*\)".*/\1/p')
[ -n "$prefix" ] || { echo "No catalog prefix for warehouse $ICEBERG_WAREHOUSE"; exit 1; }

# Storage profile of an existing warehouse brought in line with this file (idempotent)
code=$(printf '{\n%s}' "$(storage)" \
  | curl -s -o /tmp/out -w "%{http_code}" -X POST "$LAKEKEEPER_URL/management/v1/warehouse/$prefix/storage" \
    -H "Content-Type: application/json" -d @-)
[ "$code" = "200" ] || { echo "Storage profile update failed: HTTP $code $(cat /tmp/out)"; exit 1; }
echo "Storage profile of $ICEBERG_WAREHOUSE applied (remote signing off)"

code=$(curl -s -o /tmp/out -w "%{http_code}" -X POST \
  "$LAKEKEEPER_URL/catalog/v1/$prefix/namespaces" \
  -H "Content-Type: application/json" -d '{"namespace": ["default"]}')
case "$code" in
  200) echo "Namespace default created" ;;
  409) echo "Namespace default already exists" ;;
  *) echo "Namespace default creation failed: HTTP $code $(cat /tmp/out)"; exit 1 ;;
esac

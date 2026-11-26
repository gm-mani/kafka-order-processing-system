#!/usr/bin/env bash
# Checks a candidate Avro schema against the latest version in Schema Registry.
# Usage: bash scripts/check-schema-compat.sh [path/to/schema.avsc]
# Exit codes: 0 = compatible (or nothing registered yet), 1 = incompatible, 2 = error.
set -euo pipefail

REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SUBJECT="${SCHEMA_SUBJECT:-orders-topic-value}"
SCHEMA_FILE="${1:-order-events/src/main/avro/OrderCreatedEvent.avsc}"
CT="Content-Type: application/vnd.schemaregistry.v1+json"

[ -f "$SCHEMA_FILE" ] || { echo "Schema file not found: $SCHEMA_FILE"; exit 2; }

# 1. Pin the compatibility level for this subject (safe to repeat).
curl -s -o /dev/null -X PUT -H "$CT" --data '{"compatibility":"BACKWARD"}' "$REGISTRY_URL/config/$SUBJECT"

# 2. Turn the .avsc file into the escaped JSON string the registry expects.
SCHEMA_JSON=$(tr -d '\r\n\t' < "$SCHEMA_FILE" | sed 's/\\/\\\\/g; s/"/\\"/g')

# 3. Ask the registry whether the candidate can be registered.
RESPONSE=$(curl -s -X POST -H "$CT" --data "{\"schema\":\"$SCHEMA_JSON\"}" \
  "$REGISTRY_URL/compatibility/subjects/$SUBJECT/versions/latest?verbose=true")
echo "Registry response: $RESPONSE"

case "$RESPONSE" in
  *'"is_compatible":true'*)  echo "COMPATIBLE";   exit 0 ;;
  *'"is_compatible":false'*) echo "INCOMPATIBLE"; exit 1 ;;
  *'"error_code":40401'*)    echo "No schema registered yet; nothing to compare."; exit 0 ;;
  *)                         echo "Unexpected registry response"; exit 2 ;;
esac
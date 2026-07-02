#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$ROOT_DIR/config/env.dev}"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

[[ -f "$ENV_FILE" ]] || fail "Missing env file: $ENV_FILE"

set -a
# shellcheck source=/dev/null
. "$ENV_FILE"
set +a

for name in NACOS_SERVER_ADDR NACOS_NAMESPACE NACOS_GROUP NACOS_USERNAME NACOS_PASSWORD MYSQL_URL MYSQL_USER MYSQL_PASSWORD; do
  [[ -n "${!name:-}" ]] || fail "Required variable $name is missing in $ENV_FILE"
done

normalize_nacos_addr() {
  local addr="$1"
  addr="${addr#http://}"
  addr="${addr#https://}"
  addr="${addr%/}"
  addr="${addr%/nacos}"
  printf '%s\n' "$addr"
}

NACOS_SERVER_ADDR="$(normalize_nacos_addr "$NACOS_SERVER_ADDR")"
NACOS_BASE_URL="http://$NACOS_SERVER_ADDR/nacos"
DATA_ID="cardshop-trade-engine-dev.yaml"

TMP_CONFIG="$(mktemp)"
cleanup() {
  rm -f "$TMP_CONFIG"
}
trap cleanup EXIT

cat > "$TMP_CONFIG" <<YAML
spring:
  datasource:
    url: $MYSQL_URL
    username: $MYSQL_USER
    password: $MYSQL_PASSWORD
    hikari:
      maximum-pool-size: 16
      minimum-idle: 4
      connection-timeout: 2000
  liquibase:
    enabled: true
  rabbitmq:
    virtual-host: cardshop-trade-dev

trade:
  engine:
    environment: dev
    redis:
      write-enabled: true
    balance:
      owner-mode: NEW
    mq:
      publish-confirm-timeout-ms: 1000
    fault-injection:
      enabled: false
YAML

printf 'Publishing Nacos config dataId=%s namespace=%s group=%s server=%s\n' \
  "$DATA_ID" "$NACOS_NAMESPACE" "$NACOS_GROUP" "$NACOS_BASE_URL"

response="$(curl -sS -X POST "$NACOS_BASE_URL/v1/cs/configs" \
  --data-urlencode "username=$NACOS_USERNAME" \
  --data-urlencode "password=$NACOS_PASSWORD" \
  --data-urlencode "tenant=$NACOS_NAMESPACE" \
  --data-urlencode "dataId=$DATA_ID" \
  --data-urlencode "group=$NACOS_GROUP" \
  --data-urlencode "type=yaml" \
  --data-urlencode "content@$TMP_CONFIG")"

if [[ "$response" != "true" ]]; then
  fail "Nacos publish failed for $DATA_ID. Response: $response"
fi

printf 'Published Nacos config dataId=%s successfully.\n' "$DATA_ID"

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

yaml_quote() {
  local value="${1:-}"
  printf "'%s'" "${value//\'/\'\'}"
}

yaml_value() {
  local key="$1"
  local value
  value="$(sed -nE "s/^[[:space:]]*${key}:[[:space:]]*([^#]*).*/\1/p" "$CARD_SHOP_REDIS_CONFIG" | head -n 1)"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  value="${value%\"}"
  value="${value#\"}"
  value="${value%\'}"
  value="${value#\'}"
  printf '%s\n' "$value"
}

load_cardshop_dev_redis_from_nacos() {
  CARD_SHOP_DEV_NACOS_SERVER_ADDR="${CARD_SHOP_DEV_NACOS_SERVER_ADDR:-47.117.129.235:8848}"
  CARD_SHOP_DEV_NACOS_NAMESPACE="${CARD_SHOP_DEV_NACOS_NAMESPACE:-cardshop-dev}"
  CARD_SHOP_DEV_NACOS_GROUP="${CARD_SHOP_DEV_NACOS_GROUP:-dev}"
  CARD_SHOP_DEV_REDIS_DATA_ID="${CARD_SHOP_DEV_REDIS_DATA_ID:-cardshop-redis.yml}"

  for name in CARD_SHOP_DEV_NACOS_USERNAME CARD_SHOP_DEV_NACOS_PASSWORD; do
    [[ -n "${!name:-}" ]] || fail "Required variable $name is missing in $ENV_FILE when CARD_SHOP_REDIS_LOAD_FROM_CARDSHOP_NACOS=true"
  done

  CARD_SHOP_DEV_NACOS_SERVER_ADDR="$(normalize_nacos_addr "$CARD_SHOP_DEV_NACOS_SERVER_ADDR")"
  local cardshop_nacos_base_url="http://$CARD_SHOP_DEV_NACOS_SERVER_ADDR/nacos"

  printf 'Loading cardshop dev Redis config as optional helper dataId=%s namespace=%s group=%s server=%s\n' \
    "$CARD_SHOP_DEV_REDIS_DATA_ID" "$CARD_SHOP_DEV_NACOS_NAMESPACE" "$CARD_SHOP_DEV_NACOS_GROUP" "$cardshop_nacos_base_url"

  local response
  response="$(curl -sS --get "$cardshop_nacos_base_url/v1/cs/configs" \
    --data-urlencode "username=$CARD_SHOP_DEV_NACOS_USERNAME" \
    --data-urlencode "password=$CARD_SHOP_DEV_NACOS_PASSWORD" \
    --data-urlencode "tenant=$CARD_SHOP_DEV_NACOS_NAMESPACE" \
    --data-urlencode "dataId=$CARD_SHOP_DEV_REDIS_DATA_ID" \
    --data-urlencode "group=$CARD_SHOP_DEV_NACOS_GROUP")"

  [[ "$response" == *"spring:"* && "$response" == *"redis:"* ]] || fail "Failed to load cardshop dev Redis config from Nacos. Response: $response"
  printf '%s\n' "$response" > "$CARD_SHOP_REDIS_CONFIG"

  CARD_SHOP_REDIS_HOST="$(yaml_value host)"
  local redis_port redis_database redis_password redis_timeout pool_min_idle pool_max_idle pool_max_active pool_max_wait
  redis_port="$(yaml_value port)"
  redis_database="$(yaml_value database)"
  redis_password="$(yaml_value password)"
  redis_timeout="$(yaml_value timeout)"
  pool_min_idle="$(yaml_value min-idle)"
  pool_max_idle="$(yaml_value max-idle)"
  pool_max_active="$(yaml_value max-active)"
  pool_max_wait="$(yaml_value max-wait)"

  CARD_SHOP_REDIS_PORT="${redis_port:-$CARD_SHOP_REDIS_PORT}"
  CARD_SHOP_REDIS_DATABASE="${redis_database:-$CARD_SHOP_REDIS_DATABASE}"
  CARD_SHOP_REDIS_PASSWORD="${redis_password:-${CARD_SHOP_REDIS_PASSWORD:-}}"
  CARD_SHOP_REDIS_TIMEOUT="${redis_timeout:-$CARD_SHOP_REDIS_TIMEOUT}"
  CARD_SHOP_REDIS_POOL_MIN_IDLE="${pool_min_idle:-$CARD_SHOP_REDIS_POOL_MIN_IDLE}"
  CARD_SHOP_REDIS_POOL_MAX_IDLE="${pool_max_idle:-$CARD_SHOP_REDIS_POOL_MAX_IDLE}"
  CARD_SHOP_REDIS_POOL_MAX_ACTIVE="${pool_max_active:-$CARD_SHOP_REDIS_POOL_MAX_ACTIVE}"
  CARD_SHOP_REDIS_POOL_MAX_WAIT="${pool_max_wait:-$CARD_SHOP_REDIS_POOL_MAX_WAIT}"
}

load_dev_redis() {
  CARD_SHOP_REDIS_PORT="${CARD_SHOP_REDIS_PORT:-6379}"
  CARD_SHOP_REDIS_DATABASE="${CARD_SHOP_REDIS_DATABASE:-15}"
  CARD_SHOP_REDIS_TIMEOUT="${CARD_SHOP_REDIS_TIMEOUT:-3s}"
  CARD_SHOP_REDIS_POOL_MIN_IDLE="${CARD_SHOP_REDIS_POOL_MIN_IDLE:-20}"
  CARD_SHOP_REDIS_POOL_MAX_IDLE="${CARD_SHOP_REDIS_POOL_MAX_IDLE:-50}"
  CARD_SHOP_REDIS_POOL_MAX_ACTIVE="${CARD_SHOP_REDIS_POOL_MAX_ACTIVE:-100}"
  CARD_SHOP_REDIS_POOL_MAX_WAIT="${CARD_SHOP_REDIS_POOL_MAX_WAIT:-5000ms}"

  if [[ -n "${CARD_SHOP_REDIS_HOST:-}" ]]; then
    return
  fi

  if [[ "${CARD_SHOP_REDIS_LOAD_FROM_CARDSHOP_NACOS:-false}" == "true" ]]; then
    load_cardshop_dev_redis_from_nacos
    return
  fi

  fail "Required variable CARD_SHOP_REDIS_HOST is missing in $ENV_FILE. Set CARD_SHOP_REDIS_* explicitly; to use cardshop dev Nacos only as an opt-in helper, set CARD_SHOP_REDIS_LOAD_FROM_CARDSHOP_NACOS=true."
}

require_redis_value() {
  local name="$1"
  local value="$2"
  [[ -n "$value" ]] || fail "Dev Redis config is missing $name"
}

NACOS_SERVER_ADDR="$(normalize_nacos_addr "$NACOS_SERVER_ADDR")"
NACOS_BASE_URL="http://$NACOS_SERVER_ADDR/nacos"
DATA_ID="cardshop-trade-engine-dev.yaml"

TMP_CONFIG="$(mktemp)"
CARD_SHOP_REDIS_CONFIG="$(mktemp)"
cleanup() {
  rm -f "$TMP_CONFIG" "$CARD_SHOP_REDIS_CONFIG"
}
trap cleanup EXIT

load_dev_redis
require_redis_value "host" "$CARD_SHOP_REDIS_HOST"
require_redis_value "port" "$CARD_SHOP_REDIS_PORT"
require_redis_value "database" "$CARD_SHOP_REDIS_DATABASE"
require_redis_value "timeout" "$CARD_SHOP_REDIS_TIMEOUT"

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
  data:
    redis:
      # Dev Redis values are published explicitly for Spring Boot 3 spring.data.redis.*.
      host: $(yaml_quote "$CARD_SHOP_REDIS_HOST")
      port: $CARD_SHOP_REDIS_PORT
      database: $CARD_SHOP_REDIS_DATABASE
      password: $(yaml_quote "${CARD_SHOP_REDIS_PASSWORD:-}")
      timeout: $CARD_SHOP_REDIS_TIMEOUT
      lettuce:
        pool:
          min-idle: $CARD_SHOP_REDIS_POOL_MIN_IDLE
          max-idle: $CARD_SHOP_REDIS_POOL_MAX_IDLE
          max-active: $CARD_SHOP_REDIS_POOL_MAX_ACTIVE
          max-wait: $CARD_SHOP_REDIS_POOL_MAX_WAIT
  liquibase:
    enabled: true
  rabbitmq:
    virtual-host: cardshop-trade-dev
    publisher-confirm-type: correlated
    publisher-returns: true
    template:
      mandatory: true

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
    integration:
      manual-job-enabled: false
    xxl-job:
      enabled: false
      admin-addresses: \${XXL_JOB_ADMIN_ADDRESSES:}
      access-token: \${XXL_JOB_ACCESS_TOKEN:}
      executor:
        appname: cardshop-trade-engine-dev
        address:
        ip:
        port: 19999
        logpath: /data/logs/cardshop-trade-engine/xxl-job
        log-retention-days: 30
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

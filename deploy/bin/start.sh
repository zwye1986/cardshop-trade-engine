#!/usr/bin/env bash
set -euo pipefail

APP_NAME="cardshop-trade-engine"
DEFAULT_JAR_GLOB="app/cardshop-trade-engine-bootstrap-*.jar"
DEFAULT_MAC_JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_HOME"

usage() {
  cat <<'USAGE'
Usage:
  ./bin/start.sh start <dev|test|prod>
  ./bin/start.sh stop <dev|test|prod>
  ./bin/start.sh restart <dev|test|prod>
  ./bin/start.sh status <dev|test|prod>

The script loads config/env.${APP_PROFILE}. Keep only bootstrap settings there:
APP_PROFILE, SERVER_PORT, Nacos address/namespace/group/account, JDK path, and JAVA_OPTS.
USAGE
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

ACTION="${1:-}"
APP_PROFILE="${2:-${APP_PROFILE:-}}"
[[ -n "$ACTION" && -n "$APP_PROFILE" ]] || {
  usage
  exit 1
}
[[ "$ACTION" =~ ^(start|stop|restart|status)$ ]] || fail "Unsupported action '$ACTION'. Expected start|stop|restart|status."

ENV_FILE="$APP_HOME/config/env.${APP_PROFILE}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
elif [[ "$ACTION" == "status" ]]; then
  printf 'WARN: Env file is missing: %s\n' "$ENV_FILE" >&2
else
  fail "Missing env file: $ENV_FILE. Copy config/env.example to config/env.${APP_PROFILE} and fill values."
fi

APP_PROFILE="${APP_PROFILE:-$2}"
SERVER_PORT="${SERVER_PORT:-8080}"
if [[ -z "${PID_DIR:-}" ]]; then
  PID_DIR="$APP_HOME/run"
fi
if [[ -z "${LOG_DIR:-}" ]]; then
  LOG_DIR="$APP_HOME/logs"
fi
PID_FILE="$PID_DIR/$APP_NAME-$APP_PROFILE.pid"
STDOUT_LOG="$LOG_DIR/$APP_NAME-$APP_PROFILE.out"

detect_java_home() {
  local candidates=()
  if [[ -n "${CARD_SHOP_TRADE_JAVA_HOME:-}" ]]; then
    candidates+=("$CARD_SHOP_TRADE_JAVA_HOME")
  fi
  if [[ -n "${JAVA_HOME:-}" ]]; then
    candidates+=("$JAVA_HOME")
  fi
  candidates+=("/usr/local/jdk-21" "/opt/jdk-21" "$DEFAULT_MAC_JAVA_HOME")

  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -x "$candidate/bin/java" ]]; then
      local version
      version="$("$candidate/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2; exit}')"
      if [[ "$version" == 21.* ]]; then
        printf '%s\n' "$candidate"
        return 0
      fi
    fi
  done

  return 1
}

require_value() {
  local name="$1"
  local value="${!name:-}"
  [[ -n "$value" ]] || fail "Required environment variable $name is missing in $ENV_FILE."
}

normalize_nacos_addr() {
  local addr="$1"
  addr="${addr#http://}"
  addr="${addr#https://}"
  addr="${addr%/}"
  addr="${addr%/nacos}"
  printf '%s\n' "$addr"
}

resolve_jar() {
  local matches=($DEFAULT_JAR_GLOB)
  [[ ${#matches[@]} -eq 1 && -f "${matches[0]}" ]] || fail "Expected exactly one application jar under app/. Found: ${matches[*]:-none}"
  printf '%s\n' "${matches[0]}"
}

is_running() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE")"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1
}

check_port_free() {
  if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$SERVER_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "Port $SERVER_PORT is already in use. Change SERVER_PORT in $ENV_FILE or stop the occupying process."
  fi
}

start_app() {
  is_running && fail "$APP_NAME profile=$APP_PROFILE is already running. pid=$(cat "$PID_FILE")"

  require_value "NACOS_SERVER_ADDR"
  require_value "NACOS_NAMESPACE"
  require_value "NACOS_GROUP"
  require_value "NACOS_USERNAME"
  require_value "NACOS_PASSWORD"
  NACOS_SERVER_ADDR="$(normalize_nacos_addr "$NACOS_SERVER_ADDR")"

  local java_home
  java_home="$(detect_java_home)" || fail "JDK 21 is required. Set CARD_SHOP_TRADE_JAVA_HOME=/path/to/jdk21. Current JAVA_HOME=${JAVA_HOME:-<unset>}"
  export JAVA_HOME="$java_home"
  export PATH="$JAVA_HOME/bin:$PATH"

  local jar
  jar="$(resolve_jar)"
  mkdir -p "$PID_DIR" "$LOG_DIR"
  check_port_free

  printf 'Starting %s profile=%s port=%s java=%s\n' "$APP_NAME" "$APP_PROFILE" "$SERVER_PORT" "$JAVA_HOME"
  export NACOS_SERVER_ADDR NACOS_USERNAME NACOS_PASSWORD NACOS_NAMESPACE NACOS_GROUP

  nohup "$JAVA_HOME/bin/java" ${JAVA_OPTS:-} -jar "$jar" \
    --spring.profiles.active="$APP_PROFILE" \
    --server.port="$SERVER_PORT" \
    --spring.cloud.nacos.server-addr="$NACOS_SERVER_ADDR" \
    --spring.cloud.nacos.config.namespace="$NACOS_NAMESPACE" \
    --spring.cloud.nacos.config.group="$NACOS_GROUP" \
    --spring.cloud.nacos.discovery.namespace="$NACOS_NAMESPACE" \
    --spring.cloud.nacos.discovery.group="$NACOS_GROUP" \
    >>"$STDOUT_LOG" 2>&1 &

  echo $! > "$PID_FILE"
  sleep 2
  is_running || fail "Startup failed. Check log: $STDOUT_LOG"
  printf 'Started %s profile=%s pid=%s log=%s\n' "$APP_NAME" "$APP_PROFILE" "$(cat "$PID_FILE")" "$STDOUT_LOG"
}

stop_app() {
  if ! is_running; then
    printf '%s profile=%s is not running.\n' "$APP_NAME" "$APP_PROFILE"
    rm -f "$PID_FILE"
    return 0
  fi

  local pid
  pid="$(cat "$PID_FILE")"
  printf 'Stopping %s profile=%s pid=%s\n' "$APP_NAME" "$APP_PROFILE" "$pid"
  kill "$pid"
  local i
  for i in {1..30}; do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      rm -f "$PID_FILE"
      printf 'Stopped %s profile=%s\n' "$APP_NAME" "$APP_PROFILE"
      return 0
    fi
    sleep 1
  done
  fail "Process $pid did not stop within 30 seconds. Please inspect it manually."
}

status_app() {
  if is_running; then
    printf '%s profile=%s is running. pid=%s\n' "$APP_NAME" "$APP_PROFILE" "$(cat "$PID_FILE")"
  else
    printf '%s profile=%s is not running.\n' "$APP_NAME" "$APP_PROFILE"
  fi
}

case "$ACTION" in
  start)
    start_app
    ;;
  stop)
    stop_app
    ;;
  restart)
    stop_app
    start_app
    ;;
  status)
    status_app
    ;;
esac

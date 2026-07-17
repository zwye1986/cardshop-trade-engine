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

The script loads config/.${APP_PROFILE}.env. Secrets stay in environment variables
and are read by Spring placeholders; they are never appended to JVM argv.
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
REQUESTED_PROFILE="$APP_PROFILE"

ENV_FILE="$APP_HOME/config/.${APP_PROFILE}.env"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
elif [[ "$ACTION" == "status" ]]; then
  printf 'WARN: Env file is missing: %s\n' "$ENV_FILE" >&2
else
  fail "Missing env file: $ENV_FILE. Fill config/.${APP_PROFILE}.env before starting this profile."
fi

[[ "${APP_PROFILE:-}" == "$REQUESTED_PROFILE" ]] || fail "APP_PROFILE in $ENV_FILE must be '$REQUESTED_PROFILE'."
APP_PROFILE="$REQUESTED_PROFILE"
SERVER_PORT="${SERVER_PORT:-8080}"
if [[ -z "${PID_DIR:-}" ]]; then
  PID_DIR="$APP_HOME/run"
fi
if [[ -z "${LOG_DIR:-}" ]]; then
  LOG_DIR="$APP_HOME/logs"
fi
if [[ -z "${STDOUT_LOG_ENABLED:-}" ]]; then
  if [[ "$APP_PROFILE" == "prod" ]]; then
    STDOUT_LOG_ENABLED="false"
  else
    STDOUT_LOG_ENABLED="true"
  fi
fi
[[ "$STDOUT_LOG_ENABLED" =~ ^(true|false)$ ]] || fail "STDOUT_LOG_ENABLED must be true or false."
PID_FILE="$PID_DIR/$APP_NAME-$APP_PROFILE.pid"
STDOUT_LOG="$LOG_DIR/$APP_NAME-$APP_PROFILE.out"
# stdout 兜底文件轮转守护的 PID 文件
ROTATE_PID_FILE="$PID_DIR/$APP_NAME-$APP_PROFILE.rotate.pid"
# stdout 兜底文件轮转参数：与 logback 主日志策略对齐（按天 + 200MB，保留 90 天）
STDOUT_MAX_BYTES=$((200 * 1024 * 1024))
STDOUT_RETENTION_DAYS=90

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

REQUIRED_START_ENV_NAMES=(
  NACOS_SERVER_ADDR
  NACOS_USERNAME
  NACOS_PASSWORD
  NACOS_NAMESPACE
  NACOS_GROUP
  SPRING_RABBITMQ_HOST
  SPRING_RABBITMQ_PORT
  SPRING_RABBITMQ_USERNAME
  SPRING_RABBITMQ_PASSWORD
  SPRING_RABBITMQ_VIRTUAL_HOST
  V1_CARDAPI_JWT_SECRET
)

require_not_placeholder() {
  local name="$1"
  local value="${!name:-}"
  [[ "$value" != "change-me" ]] || fail "Environment variable $name still uses placeholder value in $ENV_FILE."
}

validate_start_environment() {
  local name
  for name in "${REQUIRED_START_ENV_NAMES[@]}"; do
    require_value "$name"
  done
  require_not_placeholder "NACOS_PASSWORD"
  require_not_placeholder "SPRING_RABBITMQ_PASSWORD"
  require_not_placeholder "V1_CARDAPI_JWT_SECRET"
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

# 跨平台获取文件字节数：macOS stat 用 -f%z，Linux stat 用 -c%s
file_size_bytes() {
  local f="$1"
  if stat -f%z "$f" >/dev/null 2>&1; then
    stat -f%z "$f"
  else
    stat -c%s "$f" 2>/dev/null || printf '0'
  fi
}

# 计算某天归档的下一个序号，避免同名覆盖
next_archive_seq() {
  local stem="$1"
  local seq=0
  while [[ -e "${stem}.${seq}.log" ]]; do
    seq=$((seq + 1))
  done
  printf '%s\n' "$seq"
}

# stdout 兜底文件轮转守护。参数：应用 PID、stdout 文件、日志目录、单文件上限字节、保留天数。
# 按「跨天 + 单文件达到上限」双触发 copytruncate 轮转，归档保留指定天数。
# 仅作用于 start.sh 重定向的 .out（JVM 原生输出兜底），与 logback 管控的业务日志相互独立。
# 截断瞬间极少日志可能重复或丢失，作为兜底文件可接受。
stdout_rotate_loop() {
  trap '' HUP  # 忽略终端挂断，等价 nohup，使守护在 start.sh 退出后继续运行
  local app_pid="$1"
  local stdout_log="$2"
  local log_dir="$3"
  local max_bytes="$4"
  local retention_days="$5"
  local last_day
  last_day="$(date +%Y-%m-%d)"
  while kill -0 "$app_pid" >/dev/null 2>&1; do
    sleep 60
    kill -0 "$app_pid" >/dev/null 2>&1 || break
    [[ -f "$stdout_log" ]] || continue
    local size today need_roll
    size="$(file_size_bytes "$stdout_log")"
    today="$(date +%Y-%m-%d)"
    need_roll=0
    if (( size >= max_bytes )); then
      need_roll=1
    elif [[ "$today" != "$last_day" ]]; then
      need_roll=1
    fi
    if (( need_roll )); then
      local stem archive seq
      stem="$stdout_log.$today"
      seq="$(next_archive_seq "$stem")"
      archive="${stem}.${seq}.log"
      # copytruncate：拷贝当前内容到归档，再截断原文件，使 nohup 进程
      # 继续写入同一 fd（O_APPEND），无需重启
      if cp -p "$stdout_log" "$archive" 2>/dev/null; then
        : > "$stdout_log"
        last_day="$today"
      fi
    fi
    # 清理超期归档（按 mtime），与 logback maxHistory 对齐
    find "$log_dir" -type f -name "$(basename "$stdout_log").*.log" -mtime +"$retention_days" -delete 2>/dev/null
  done
}

# 停止 stdout 轮转守护并清理其 PID 文件
stop_stdout_rotate() {
  if [[ -f "$ROTATE_PID_FILE" ]]; then
    local rpid
    rpid="$(cat "$ROTATE_PID_FILE")"
    if [[ -n "$rpid" ]] && kill -0 "$rpid" >/dev/null 2>&1; then
      kill "$rpid" 2>/dev/null || true
    fi
    rm -f "$ROTATE_PID_FILE"
  fi
}

start_app() {
  is_running && fail "$APP_NAME profile=$APP_PROFILE is already running. pid=$(cat "$PID_FILE")"

  validate_start_environment
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
  export APP_PROFILE SERVER_PORT
  export NACOS_SERVER_ADDR NACOS_USERNAME NACOS_PASSWORD NACOS_NAMESPACE NACOS_GROUP
  export SPRING_RABBITMQ_HOST SPRING_RABBITMQ_PORT SPRING_RABBITMQ_USERNAME SPRING_RABBITMQ_PASSWORD SPRING_RABBITMQ_VIRTUAL_HOST
  export LEGACY_CARD_SHOP_BASE_URL V1_CARDAPI_JWT_SECRET
  # 导出 LOG_DIR，使 application.yml 的 logging.file.path:${LOG_DIR:./logs}
  # 能取到部署脚本约定的日志目录，并注入为 logback 的 LOG_PATH 系统属性
  export LOG_DIR

  if [[ "$STDOUT_LOG_ENABLED" == "true" ]]; then
    nohup "$JAVA_HOME/bin/java" ${JAVA_OPTS:-} -jar "$jar" >>"$STDOUT_LOG" 2>&1 &
  else
    # 生产默认禁用 stdout 兜底文件，避免 nohup 创建 cardshop-trade-engine-prod.out。
    # 业务日志仍由 logback 写入 $LOG_DIR/cardshop-trade-engine.log；早期 JVM 诊断请临时打开 STDOUT_LOG_ENABLED。
    nohup "$JAVA_HOME/bin/java" ${JAVA_OPTS:-} -jar "$jar" >/dev/null 2>&1 &
  fi
  local app_pid=$!
  echo "$app_pid" > "$PID_FILE"
  sleep 2
  if [[ "$STDOUT_LOG_ENABLED" == "true" ]]; then
    is_running || fail "Startup failed. Check log: $STDOUT_LOG"
  else
    is_running || fail "Startup failed. Check primary log: $LOG_DIR/$APP_NAME.log"
  fi

  if [[ "$STDOUT_LOG_ENABLED" == "true" ]]; then
    # 启动 stdout 兜底文件轮转守护：按天 + 200MB copytruncate 轮转，归档保留 90 天。
    # 后台子 shell 继承本脚本的函数与变量；函数内 trap '' HUP 忽略终端挂断，
    # 等价 nohup，start.sh 退出后由 init 接管继续运行，随应用进程退出而自退。
    stdout_rotate_loop "$app_pid" "$STDOUT_LOG" "$LOG_DIR" "$STDOUT_MAX_BYTES" "$STDOUT_RETENTION_DAYS" &
    echo $! > "$ROTATE_PID_FILE"
    printf 'Started %s profile=%s pid=%s log=%s\n' "$APP_NAME" "$APP_PROFILE" "$app_pid" "$STDOUT_LOG"
  else
    rm -f "$ROTATE_PID_FILE"
    printf 'Started %s profile=%s pid=%s stdout=disabled log=%s\n' "$APP_NAME" "$APP_PROFILE" "$app_pid" "$LOG_DIR/$APP_NAME.log"
  fi
}

stop_app() {
  if ! is_running; then
    printf '%s profile=%s is not running.\n' "$APP_NAME" "$APP_PROFILE"
    rm -f "$PID_FILE" "$ROTATE_PID_FILE"
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
      stop_stdout_rotate
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

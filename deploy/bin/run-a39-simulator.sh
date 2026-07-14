#!/usr/bin/env bash
set -euo pipefail

APP_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_MAC_JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home"
ENV_FILE="$APP_HOME/config/a39-simulator.env"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

detect_java_home() {
  local candidates=()
  if [[ -n "${CARD_SHOP_TRADE_JAVA_HOME:-}" ]]; then
    candidates+=("$CARD_SHOP_TRADE_JAVA_HOME")
  fi
  if [[ -n "${JAVA_HOME:-}" ]]; then
    candidates+=("$JAVA_HOME")
  fi
  candidates+=("/opt/java/jdk-21.0.11" "/usr/local/jdk-21" "/opt/jdk-21" "$DEFAULT_MAC_JAVA_HOME")

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

JAVA_HOME="$(detect_java_home)" || fail "JDK 21 is required for A39 simulator. Set CARD_SHOP_TRADE_JAVA_HOME=/path/to/jdk21 or JAVA_HOME=/path/to/jdk21 in $ENV_FILE. Current JAVA_HOME=${JAVA_HOME:-<unset>}"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

JARS=("$APP_HOME"/app/cardshop-trade-engine-simulator-*.jar)
if [[ ! -f "${JARS[0]}" ]]; then
  printf 'ERROR: simulator jar not found under %s/app\n' "$APP_HOME" >&2
  exit 1
fi

exec "$JAVA_HOME/bin/java" -jar "${JARS[0]}" "$@"

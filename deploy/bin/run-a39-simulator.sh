#!/usr/bin/env bash
set -euo pipefail

APP_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$APP_HOME/config/a39-simulator.env"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/java}"
if [[ -z "${JAVA_BIN:-}" || ! -x "$JAVA_BIN" ]]; then
  JAVA_BIN="java"
fi

JARS=("$APP_HOME"/app/cardshop-trade-engine-simulator-*.jar)
if [[ ! -f "${JARS[0]}" ]]; then
  printf 'ERROR: simulator jar not found under %s/app\n' "$APP_HOME" >&2
  exit 1
fi

exec "$JAVA_BIN" -jar "${JARS[0]}" "$@"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

APP_NAME="cardshop-trade-engine"
BOOTSTRAP_MODULE="cardshop-trade-engine-bootstrap"
DEFAULT_MAC_JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

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

JAVA_HOME="$(detect_java_home)" || fail "JDK 21 is required for packaging. Set CARD_SHOP_TRADE_JAVA_HOME=/path/to/jdk21. Current JAVA_HOME=${JAVA_HOME:-<unset>}"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

VERSION="$(awk -F'[<>]' '/<artifactId>cardshop-trade-engine<\/artifactId>/{found=1; next} found && /<version>/{print $3; exit}' pom.xml)"
[[ -n "$VERSION" ]] || fail "Unable to resolve Maven project.version."

RELEASE_DIR="$ROOT_DIR/target/release/$APP_NAME-$VERSION"
ZIP_PATH="$ROOT_DIR/dist/$APP_NAME-$VERSION.zip"
JAR_PATH="$ROOT_DIR/$BOOTSTRAP_MODULE/target/$BOOTSTRAP_MODULE-$VERSION.jar"

printf 'Packaging %s version=%s with JDK 21 at %s\n' "$APP_NAME" "$VERSION" "$JAVA_HOME"
mvn clean package

[[ -f "$JAR_PATH" ]] || fail "Application jar not found: $JAR_PATH"

rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR/app" "$RELEASE_DIR/bin" "$RELEASE_DIR/config" "$RELEASE_DIR/logs" "$RELEASE_DIR/nacos" "$ROOT_DIR/dist"

cp "$JAR_PATH" "$RELEASE_DIR/app/"
cp "$ROOT_DIR/deploy/bin/start.sh" "$RELEASE_DIR/bin/start.sh"
cp "$ROOT_DIR/deploy/config/env.example" "$RELEASE_DIR/config/env.example"
cp "$ROOT_DIR/deploy/nacos/cardshop-trade-engine-dev.example.yaml" "$RELEASE_DIR/nacos/cardshop-trade-engine-dev.example.yaml"
cp "$ROOT_DIR/deploy/STARTUP.md" "$RELEASE_DIR/STARTUP.md"
chmod +x "$RELEASE_DIR/bin/start.sh"

(
  cd "$ROOT_DIR/target/release"
  zip -qr "$ZIP_PATH" "$APP_NAME-$VERSION"
)

printf 'Created deployment package: %s\n' "$ZIP_PATH"

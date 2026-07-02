#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export JAVA_HOME="${CARD_SHOP_TRADE_JAVA_HOME:-/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home}"
export PATH="$JAVA_HOME/bin:$PATH"
export APP_PROFILE="${APP_PROFILE:-local}"
export SERVER_PORT="${SERVER_PORT:-18081}"

export SPRING_CLOUD_NACOS_CONFIG_ENABLED=false
export SPRING_CLOUD_NACOS_DISCOVERY_ENABLED=false
export SPRING_CLOUD_DISCOVERY_ENABLED=false

mvn -pl cardshop-trade-engine-bootstrap -am -DskipTests package

exec java -jar cardshop-trade-engine-bootstrap/target/cardshop-trade-engine-bootstrap-0.1.0-SNAPSHOT.jar \
  --spring.profiles.active="$APP_PROFILE" \
  --spring.config.import= \
  --server.port="$SERVER_PORT" \
  --spring.cloud.nacos.config.enabled=false \
  --spring.cloud.nacos.discovery.enabled=false \
  --spring.cloud.discovery.enabled=false

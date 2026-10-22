#!/usr/bin/env bash
set -euo pipefail

# 参见 docs/交行直充业务系统详细设计.md「交行 2104 通知实施细化」。
# 私有银行 SDK 不入库；只允许安装已审计的官方 2.2.6 二进制及显式依赖描述。
readonly EXPECTED_SHA256='8df82ae2931dd5cc52fcbbea5643cd6819412f27f9cf64478b476c2e53120fad'
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ $# -ne 1 ]]; then
    echo '用法: scripts/install-bocom-sdk.sh /path/to/bocom-openapi-sdk-2.2.6.jar' >&2
    exit 1
fi
sdk_jar="$1"
if [[ -d "$sdk_jar" ]]; then
    sdk_jar="$sdk_jar/lib/bocom-openapi-sdk-2.2.6.jar"
fi
if [[ ! -f "$sdk_jar" ]]; then
    echo '指定位置不存在官方SDK JAR。' >&2
    exit 1
fi
actual_sha256="$(shasum -a 256 -- "$sdk_jar")"
actual_sha256="${actual_sha256%% *}"
if [[ "$actual_sha256" != "$EXPECTED_SHA256" ]]; then
    echo 'SDK SHA-256 校验失败，拒绝安装。' >&2
    exit 1
fi
mvn org.apache.maven.plugins:maven-install-plugin:3.1.3:install-file \
    "-Dfile=$sdk_jar" "-DpomFile=$SCRIPT_DIR/bocom-openapi-sdk.pom.xml" -DgeneratePom=false

#!/usr/bin/env bash
# 可丢弃MySQL结构夹具；参见 docs/交行直充业务系统详细设计.md「9.8 名创单入口、共库核销分流」。
# 必需：MINISO_MYSQL_IMAGE（预先拉取的8.0.28镜像）、JAVA21_HOME（或指向JDK21的JAVA_HOME）。
# 可选：MINISO_MYSQL_ISOLATION=READ-COMMITTED|REPEATABLE-READ；默认与test188全局默认RC一致。
# 旧端消费同一schema：另设MINISO_LEGACY_DIR及JAVA8_HOME。未指定时明确报告旧端未验收。
# 不接受远端JDBC URL、已有容器或共享schema；退出只删除本次容器及临时文件，不执行DROP DATABASE。
set +x
set -euo pipefail

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
[[ $# == 1 && "$1" == --schema ]] || fail 'usage: test-miniso-shared-callback.sh --schema'
: "${MINISO_MYSQL_IMAGE:?必须显式设置已核实版本的MINISO_MYSQL_IMAGE（8.0.28）}"
[[ -z "${MINISO_MYSQL_JDBC_URL:-}${MINISO_MYSQL_HOST:-}" ]] || fail '不接受外部数据库地址'
ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
JAVA21_HOME="${JAVA21_HOME:-${JAVA_HOME:-}}"
[[ -n "$JAVA21_HOME" && -x "$JAVA21_HOME/bin/javac" ]] || fail '需要JAVA21_HOME/JAVA_HOME指向JDK21'
[[ "$("$JAVA21_HOME/bin/javac" -version 2>&1)" == javac\ 21.* ]] || fail '新端必须使用JDK21'
if [[ -n "${MINISO_LEGACY_DIR:-}" ]]; then
    [[ -f "$MINISO_LEGACY_DIR/cardShop-admin/src/test/java/com/ruoyi/web/mc/miniso/bridge/MinisoBridgeMysqlContractTest.java" ]] \
        || fail '旧工作区缺少MySQL合同测试，不能报告旧端通过'
    [[ -n "${JAVA8_HOME:-}" && -x "$JAVA8_HOME/bin/javac" ]] || fail '旧端需要JAVA8_HOME指向JDK8（不能是JRE）'
    [[ "$("$JAVA8_HOME/bin/javac" -version 2>&1)" == javac\ 1.8.* ]] || fail '旧端必须使用JDK8'
fi
for tool in docker mvn openssl; do command -v "$tool" >/dev/null || fail "缺少$tool"; done
# Docker的显式context优先于DOCKER_HOST，必须核验实际生效端点，不能被本地HOST值掩盖远程context。
# 回环端口必须属于本机Docker；参见详细设计9.8的可丢弃环境边界。
if [[ -n "${DOCKER_CONTEXT:-}" ]]; then
    docker_endpoint="$(docker context inspect "$DOCKER_CONTEXT" --format '{{.Endpoints.docker.Host}}')"
else
    docker_endpoint="${DOCKER_HOST:-$(docker context inspect --format '{{.Endpoints.docker.Host}}')}"
fi
[[ "$docker_endpoint" == unix://* ]] || fail '夹具只允许本机unix socket Docker'
docker info >/dev/null 2>&1 || fail 'Docker daemon不可用'
image_id="$(docker image inspect "$MINISO_MYSQL_IMAGE" --format '{{.Id}}')" || fail '镜像不存在，请先显式拉取8.0.28'
platform="$(docker image inspect "$image_id" --format '{{.Os}}/{{.Architecture}}')"
export MINISO_MYSQL_ISOLATION="${MINISO_MYSQL_ISOLATION:-READ-COMMITTED}"
case "$MINISO_MYSQL_ISOLATION" in
    READ-COMMITTED|REPEATABLE-READ) ;;
    *) fail '隔离级别只允许READ-COMMITTED或REPEATABLE-READ' ;;
esac

umask 077
scratch="$(mktemp -d "${TMPDIR:-/tmp}/miniso-mysql.XXXXXX")"
export MINISO_MYSQL_RUN_ID="$(openssl rand -hex 8)"
export MINISO_MYSQL_SCHEMA="miniso_bridge_it_$MINISO_MYSQL_RUN_ID"
export MINISO_MYSQL_PASSWORD="$(openssl rand -hex 24)"
cleanup() {
    result=$?
    trap - EXIT
    if [[ -s "$scratch/container.id" ]]; then
        container_id="$(cat "$scratch/container.id")"
        if ! docker rm -fv "$container_id" >/dev/null; then
            printf 'ERROR: 本次容器清理失败，需核查容器%s\n' "$container_id" >&2
            result=1
        fi
    fi
    rm -rf "$scratch"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cat > "$scratch/mysql.env" <<EOF
MYSQL_ROOT_PASSWORD=$(openssl rand -hex 24)
MYSQL_DATABASE=$MINISO_MYSQL_SCHEMA
MYSQL_USER=miniso_bridge_it
MYSQL_PASSWORD=$MINISO_MYSQL_PASSWORD
EOF
# 不复用卷。固定镜像ID，避免同名tag变化；amd64旧镜像在arm64上显式使用Docker仿真。
docker create --cidfile "$scratch/container.id" --platform "$platform" --pull never \
    --label "miniso.fixture=$MINISO_MYSQL_RUN_ID" --env-file "$scratch/mysql.env" \
    --publish 127.0.0.1::3306 --tmpfs /var/lib/mysql:rw \
    "$image_id" --transaction-isolation="$MINISO_MYSQL_ISOLATION" >/dev/null
container_id="$(cat "$scratch/container.id")"
docker start "$container_id" >/dev/null
mysql_fixture() {
    docker exec -i -e MYSQL_PWD="$MINISO_MYSQL_PASSWORD" "$container_id" \
        mysql --protocol=TCP -h127.0.0.1 -uminiso_bridge_it "$MINISO_MYSQL_SCHEMA" "$@"
}
ready=false
for ((attempt = 0; attempt < 120; attempt++)); do
    if mysql_fixture -Nse 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi
    sleep 1
done
if [[ "$ready" != true ]]; then docker logs --tail 40 "$container_id" >&2; fail 'MySQL启动超时'; fi
export MINISO_MYSQL_PORT="$(docker port "$container_id" 3306/tcp | sed -n 's/^127\.0\.0\.1://p')"
[[ "$MINISO_MYSQL_PORT" =~ ^[0-9]+$ ]] || fail '未取得本次随机回环端口'
mysql_fixture <<EOF
CREATE TABLE miniso_bridge_fixture (run_id char(16) NOT NULL PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO miniso_bridge_fixture VALUES ('$MINISO_MYSQL_RUN_ID');
EOF
printf 'MINISO fixture: image=%s platform=%s schema=%s isolation=%s\n' \
    "$image_id" "$platform" "$MINISO_MYSQL_SCHEMA" "$MINISO_MYSQL_ISOLATION"
JAVA_HOME="$JAVA21_HOME" PATH="$JAVA21_HOME/bin:$PATH" \
    mvn -B -f "$ROOT/pom.xml" -pl cardshop-trade-engine-infrastructure -am \
    -Dmaven.test.skip=false -DskipTests=false \
    -Dtest=MinisoBridgeMysqlContractTest -Dsurefire.failIfNoSpecifiedTests=false \
    -Dminiso.mysqlTests=true test
if [[ -n "${MINISO_LEGACY_DIR:-}" ]]; then
    JAVA_HOME="$JAVA8_HOME" PATH="$JAVA8_HOME/bin:$PATH" \
        mvn -B -f "$MINISO_LEGACY_DIR/pom.xml" -pl cardShop-admin -am \
        -Dmaven.test.skip=false -DskipTests=false -Dtest=MinisoBridgeMysqlContractTest \
        -Dsurefire.failIfNoSpecifiedTests=false -Dminiso.mysqlTests=true test
    printf 'PASS: 新端结构迁移及旧端同schema消费（不是资金并发验收）\n'
else
    printf 'PASS: 新端结构迁移；NOT RUN: 旧端消费（未设置MINISO_LEGACY_DIR，不计通过）\n'
fi

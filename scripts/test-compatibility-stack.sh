#!/usr/bin/env bash
# 真实HTTP/Redis/RabbitMQ/MySQL隔离验证，只创建本机一次性资源；参见详细设计8.2.1及集成测试记录。
set +x
set -euo pipefail
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
[[ $# == 0 ]] || fail '不接受现有环境或远端连接参数'
ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
JAVA21_HOME="${JAVA21_HOME:-${JAVA_HOME:-$(/usr/libexec/java_home -v 21 2>/dev/null || true)}}"
[[ -x "$JAVA21_HOME/bin/javac" && "$("$JAVA21_HOME/bin/javac" -version 2>&1)" == javac\ 21.* ]] || fail '需要JDK21'
for tool in docker mvn openssl python3; do command -v "$tool" >/dev/null || fail "缺少$tool"; done
if [[ -n "${DOCKER_CONTEXT:-}" ]]; then
    endpoint="$(docker context inspect "$DOCKER_CONTEXT" --format '{{.Endpoints.docker.Host}}')"
else
    endpoint="${DOCKER_HOST:-$(docker context inspect --format '{{.Endpoints.docker.Host}}')}"
fi
[[ "$endpoint" == unix://* ]] || fail '只允许本机Docker unix socket'
mysql_image="$(docker image inspect mysql:8.0.36 --format '{{index .RepoDigests 0}}')"
redis_image="$(docker image inspect redis:7.4 --format '{{index .RepoDigests 0}}')"
rabbit_image="$(docker image inspect rabbitmq:3.13-management --format '{{index .RepoDigests 0}}')"
umask 077
scratch="$(mktemp -d "${TMPDIR:-/tmp}/trade-compat-stack.XXXXXX")"
containers=()
cleanup() {
    result=$?
    trap - EXIT
    for container in "${containers[@]}"; do
        if ! docker rm -fv "$container" >/dev/null; then result=1; fi
    done
    rm -rf "$scratch"
    printf 'CLEANED disposable stack fixture, result=%s\n' "$result"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export COMPAT_STACK_RUN="$(openssl rand -hex 16)"
export COMPAT_STACK_PASSWORD="$(openssl rand -hex 24)"
export COMPAT_STACK_BASELINE="$ROOT/cardshop-trade-engine-infrastructure/src/test/resources/miniso-bridge/legacy-baseline.sql"
printf 'MYSQL_ROOT_PASSWORD=%s\nMYSQL_ROOT_HOST=%%\n' "$COMPAT_STACK_PASSWORD" > "$scratch/mysql.env"
printf 'RABBITMQ_DEFAULT_USER=compat_it\nRABBITMQ_DEFAULT_PASS=%s\nRABBITMQ_DEFAULT_VHOST=compat_%s\n' \
    "$COMPAT_STACK_PASSWORD" "$COMPAT_STACK_RUN" > "$scratch/rabbit.env"
mysql="$(docker run -d --pull never --platform linux/amd64 --env-file "$scratch/mysql.env" \
    -p 127.0.0.1::3306 "$mysql_image" --lower-case-table-names=1 --transaction-isolation=READ-COMMITTED \
    --character-set-server=utf8mb3 --collation-server=utf8mb3_general_ci)"
containers+=("$mysql")
redis="$(docker run -d --pull never -p 127.0.0.1::6379 "$redis_image" redis-server --save '' --appendonly no)"
containers+=("$redis")
rabbit="$(docker run -d --pull never --env-file "$scratch/rabbit.env" \
    -p 127.0.0.1::5672 -p 127.0.0.1::15672 "$rabbit_image")"
containers+=("$rabbit")
export COMPAT_STACK_MYSQL_PORT="$(docker port "$mysql" 3306/tcp | sed -n 's/^127\.0\.0\.1://p')"
export COMPAT_STACK_REDIS_PORT="$(docker port "$redis" 6379/tcp | sed -n 's/^127\.0\.0\.1://p')"
export COMPAT_STACK_RABBIT_PORT="$(docker port "$rabbit" 5672/tcp | sed -n 's/^127\.0\.0\.1://p')"
export COMPAT_STACK_MANAGEMENT_PORT="$(docker port "$rabbit" 15672/tcp | sed -n 's/^127\.0\.0\.1://p')"
ready=false
for ((attempt=0; attempt<120; attempt++)); do
    if docker exec -e MYSQL_PWD="$COMPAT_STACK_PASSWORD" "$mysql" mysql -uroot -Nse 'SELECT 1' >/dev/null 2>&1 \
        && docker exec "$redis" redis-cli ping >/dev/null 2>&1 \
        && docker exec "$rabbit" rabbitmq-diagnostics -q check_running >/dev/null 2>&1; then
        ready=true; break
    fi
    sleep 1
done
[[ "$ready" == true ]] || fail '一次性中间件启动超时'
docker exec "$redis" redis-cli set compat-stack-fixture "$COMPAT_STACK_RUN" >/dev/null
printf 'STACK images mysql=%s redis=%s rabbit=%s\n' "$mysql_image" "$redis_image" "$rabbit_image"
printf 'STACK run=%s loopbackPorts mysql=%s redis=%s amqp=%s management=%s\n' \
    "$COMPAT_STACK_RUN" "$COMPAT_STACK_MYSQL_PORT" "$COMPAT_STACK_REDIS_PORT" "$COMPAT_STACK_RABBIT_PORT" "$COMPAT_STACK_MANAGEMENT_PORT"
JAVA_HOME="$JAVA21_HOME" PATH="$JAVA21_HOME/bin:$PATH" mvn -B -f "$ROOT/pom.xml" \
    -pl cardshop-trade-engine-bootstrap -am -Dproduction.compatibility.stackTests=true \
    -Dsurefire.failIfNoSpecifiedTests=false -Dtest=ProductionCompatibilityStackIntegrationTest test
python3 - "$ROOT/cardshop-trade-engine-bootstrap/target/surefire-reports/TEST-com.cardshop.trade.engine.bootstrap.config.ProductionCompatibilityStackIntegrationTest.xml" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
assert int(r.attrib['tests']) == 4, r.attrib
assert all(int(r.attrib.get(k, 0)) == 0 for k in ('failures', 'errors', 'skipped')), r.attrib
print('VERIFIED:', r.attrib['name'], 'tests=4 failures=0 errors=0 skipped=0')
PY
printf 'PASS: HTTP occupancy, writer retry, SIGKILL recovery, version advance/stale mark/old MQ replay; synthetic local data only\n'

#!/usr/bin/env bash
# 同步作废隔离验证，参见详细设计9.3。只新建本机无卷容器，不接受外部数据库/现成容器。
set -euo pipefail
set +x
ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
JAVA_HOME="${JAVA21_HOME:-$(/usr/libexec/java_home -v 21)}"
export JAVA_HOME PATH="$JAVA_HOME/bin:$PATH"
[[ "$(javac -version 2>&1)" == javac\ 21.* ]] || { echo '需要JDK21' >&2; exit 1; }
[[ -z "${MINISO_MYSQL_JDBC_URL:-}${MINISO_MYSQL_HOST:-}" ]] || { echo '禁止外部数据库' >&2; exit 1; }
if [[ -n "${DOCKER_CONTEXT:-}" ]]; then
    endpoint="$(docker context inspect "$DOCKER_CONTEXT" --format '{{.Endpoints.docker.Host}}')"
else
    endpoint="${DOCKER_HOST:-$(docker context inspect --format '{{.Endpoints.docker.Host}}')}"
fi
[[ "$endpoint" == unix://* ]] || { echo '只允许本机Docker' >&2; exit 1; }
image="$(docker image inspect "${MINISO_MYSQL_IMAGE:-mysql:8.0.28}" --format '{{.Id}}')"
platform="$(docker image inspect "$image" --format '{{.Os}}/{{.Architecture}}')"

verify_report() {
    python3 - "$1" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
assert int(r.attrib['tests']) == 1, r.attrib
assert all(int(r.attrib.get(k, 0)) == 0 for k in ('failures', 'errors', 'skipped')), r.attrib
print('VERIFIED', r.attrib['name'], 'tests=1 failures=0 errors=0 skipped=0')
PY
}

run_fixture() (
    umask 077
    scratch="$(mktemp -d "${TMPDIR:-/tmp}/sinv.XXXXXX")"
    cleanup() {
        result=$?
        trap - EXIT
        if [[ -s "$scratch/container.id" ]]; then docker rm -fv "$(cat "$scratch/container.id")" >/dev/null || result=1; fi
        rm -rf "$scratch"
        echo "CLEANED SINV fixture isolation=$MINISO_MYSQL_ISOLATION result=$result"
        exit "$result"
    }
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    export MINISO_MYSQL_RUN_ID="$(openssl rand -hex 8)"
    export MINISO_MYSQL_SCHEMA="miniso_bridge_it_$MINISO_MYSQL_RUN_ID"
    export MINISO_MYSQL_PASSWORD="$(openssl rand -hex 24)"
    cat > "$scratch/mysql.env" <<EOF
MYSQL_ROOT_PASSWORD=$(openssl rand -hex 24)
MYSQL_DATABASE=$MINISO_MYSQL_SCHEMA
MYSQL_USER=miniso_bridge_it
MYSQL_PASSWORD=$MINISO_MYSQL_PASSWORD
EOF
    docker create --cidfile "$scratch/container.id" --platform "$platform" --pull never \
        --label "sinv.fixture=$MINISO_MYSQL_RUN_ID" --env-file "$scratch/mysql.env" \
        --publish 127.0.0.1::3306 --tmpfs /var/lib/mysql:rw "$image" \
        --transaction-isolation="$MINISO_MYSQL_ISOLATION" --skip-log-bin >/dev/null
    container="$(cat "$scratch/container.id")"
    docker start "$container" >/dev/null
    mysql_fixture() {
        docker exec -i -e MYSQL_PWD="$MINISO_MYSQL_PASSWORD" "$container" \
            mysql --protocol=TCP -h127.0.0.1 -uminiso_bridge_it "$MINISO_MYSQL_SCHEMA" "$@"
    }
    ready=false
    for ((attempt=0; attempt<120; attempt++)); do
        if mysql_fixture -Nse 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi
        sleep 1
    done
    [[ "$ready" == true ]] || { echo '本次MySQL未就绪' >&2; exit 1; }
    export MINISO_MYSQL_PORT="$(docker port "$container" 3306/tcp | sed -n 's/^127\.0\.0\.1://p')"
    [[ "$MINISO_MYSQL_PORT" =~ ^[0-9]+$ ]] || exit 1
    mysql_fixture <<EOF
CREATE TABLE miniso_bridge_fixture (run_id char(16) NOT NULL PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO miniso_bridge_fixture VALUES ('$MINISO_MYSQL_RUN_ID');
EOF
    echo "SINV fixture image=$image schema=$MINISO_MYSQL_SCHEMA isolation=$MINISO_MYSQL_ISOLATION"
    mvn -B -f "$ROOT/pom.xml" -pl cardshop-trade-engine-infrastructure -am \
        -Dtest='MinisoBridgeMysqlContractTest#actualMaster*' -Dsurefire.failIfNoSpecifiedTests=false -Dminiso.mysqlTests=true test
    verify_report "$ROOT/cardshop-trade-engine-infrastructure/target/surefire-reports/TEST-com.cardshop.trade.engine.infrastructure.persistence.MinisoBridgeMysqlContractTest.xml"
    mvn -B -f "$ROOT/pom.xml" -pl cardshop-trade-engine-bootstrap -am \
        -Dtest=V1SynchronousInvalidationIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false -Dminiso.mysqlTests=true test
    verify_report "$ROOT/cardshop-trade-engine-bootstrap/target/surefire-reports/TEST-com.cardshop.trade.engine.bootstrap.config.V1SynchronousInvalidationIntegrationTest.xml"
    # 复用真实资金恢复合同，两次独立Surefire JVM共享本次可丢弃库；最后才运行带回滚检查的恢复轮次。
    for resume in false true; do
        mvn -B -f "$ROOT/pom.xml" -pl cardshop-trade-engine-infrastructure -am \
            -Dtest='MinisoBridgeMysqlContractTest#recovers*' -Dsurefire.failIfNoSpecifiedTests=false \
            -Dminiso.mysqlTests=true -Dminiso.recoveryTests=true -Dminiso.noReleaseResume="$resume" test
        verify_report "$ROOT/cardshop-trade-engine-infrastructure/target/surefire-reports/TEST-com.cardshop.trade.engine.infrastructure.persistence.MinisoBridgeMysqlContractTest.xml"
    done
)
for isolation in READ-COMMITTED REPEATABLE-READ; do
    export MINISO_MYSQL_ISOLATION="$isolation"
    run_fixture
done

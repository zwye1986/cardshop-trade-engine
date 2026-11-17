#!/usr/bin/env bash
# 可丢弃MySQL8.0.36迁移/订单身份/跨JVM恢复验证，禁止接收现有数据库地址；参见详细设计8.2.1。
set +x
set -euo pipefail
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
legacy_repo=""
legacy_xxl=false
legacy_xxl_source=false
if [[ $# != 0 ]]; then
    [[ $# == 2 && ( "$1" == --legacy-repo || "$1" == --legacy-xxl-repo || "$1" == --legacy-xxl-source-repo ) && -d "$2/.git" ]] \
        || fail '只接受 --legacy-repo/--legacy-xxl-repo/--legacy-xxl-source-repo 本地旧仓库路径，不接受数据库参数'
    if [[ "$1" == --legacy-xxl-repo || "$1" == --legacy-xxl-source-repo ]]; then legacy_xxl=true; fi
    if [[ "$1" == --legacy-xxl-source-repo ]]; then legacy_xxl_source=true; fi
    legacy_repo="$(git -C "$2" rev-parse --show-toplevel)"
fi
ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
JAVA21_HOME="${JAVA21_HOME:-${JAVA_HOME:-$(/usr/libexec/java_home -v 21 2>/dev/null || true)}}"
[[ -x "$JAVA21_HOME/bin/javac" ]] || fail '请设置JAVA21_HOME为JDK21'
[[ "$("$JAVA21_HOME/bin/javac" -version 2>&1)" == javac\ 21.* ]] || fail '需要JDK21'
if [[ -n "$legacy_repo" ]]; then
    JAVA8_HOME="${JAVA8_HOME:-$(/usr/libexec/java_home -v 1.8 2>/dev/null || true)}"
    [[ -x "$JAVA8_HOME/bin/javac" && "$("$JAVA8_HOME/bin/javac" -version 2>&1)" == javac\ 1.8.* ]] || fail '旧候选需要JDK8'
    [[ -f "$legacy_repo/cardShop-admin/target/cardShop-admin.jar" \
        && -f "$legacy_repo/cardShop-admin/src/test/resources/transfer/legacy-schema.sql" ]] || fail '缺本地候选JAR或脱敏metadata夹具'
fi
for tool in docker mvn openssl python3; do command -v "$tool" >/dev/null || fail "缺少$tool"; done
if [[ -n "${DOCKER_CONTEXT:-}" ]]; then
    endpoint="$(docker context inspect "$DOCKER_CONTEXT" --format '{{.Endpoints.docker.Host}}')"
else
    endpoint="${DOCKER_HOST:-$(docker context inspect --format '{{.Endpoints.docker.Host}}')}"
fi
[[ "$endpoint" == unix://* ]] || fail '只允许本机Docker unix socket'
# 该版本使用已核实的amd64制品；显式选平台，避免Docker29的OCI索引返回空Architecture。
platform=linux/amd64
image="$(docker image inspect mysql:8.0.36 --format '{{index .RepoDigests 0}}')" \
    || fail '请先执行 docker pull --platform linux/amd64 mysql:8.0.36'
docker image inspect --platform "$platform" "$image" >/dev/null \
    || fail '本机镜像缺少linux/amd64制品'
umask 077
scratch="$(mktemp -d "${TMPDIR:-/tmp}/trade-compat.XXXXXX")"
container=""
cleanup() {
    result=$?
    trap - EXIT
    if [[ -n "$container" ]]; then
        if ! docker rm -fv "$container" >/dev/null; then result=1; fi
    fi
    rm -rf "$scratch"
    printf 'CLEANED disposable compatibility fixture, result=%s\n' "$result"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [[ -n "$legacy_repo" ]]; then
    # 原样复制候选及其内嵌依赖到临时目录；只编译验证探针，绝不重建/覆盖旧端制品。
    export COMPAT_LEGACY_WORK="$scratch"
    export COMPAT_LEGACY_BASELINE="$scratch/legacy-schema.sql"
    export COMPAT_LEGACY_JAVA="$JAVA8_HOME/bin/java"
    export COMPAT_LEGACY_LOGBACK="$scratch/logback.xml"
    export COMPAT_LEGACY_CLASSPATH="$scratch/probe:$scratch/candidate/BOOT-INF/classes:$scratch/candidate/BOOT-INF/lib/*"
    mkdir "$scratch/probe" "$scratch/candidate"
    cp "$legacy_repo/cardShop-admin/src/test/resources/transfer/legacy-schema.sql" "$COMPAT_LEGACY_BASELINE"
    python3 - "$legacy_repo/cardShop-admin/target/cardShop-admin.jar" "$scratch/candidate" "$COMPAT_LEGACY_BASELINE" <<'PY'
import hashlib, pathlib, sys, zipfile
jar, destination, baseline = map(pathlib.Path, sys.argv[1:])
data = jar.read_bytes()
with zipfile.ZipFile(jar) as archive:
    for name in archive.namelist():
        if name.startswith(('BOOT-INF/classes/', 'BOOT-INF/lib/')):
            archive.extract(name, destination)
print('LEGACY candidate SHA256=' + hashlib.sha256(data).hexdigest())
print('LEGACY metadata SHA256=' + hashlib.sha256(baseline.read_bytes()).hexdigest())
PY
    if [[ "$legacy_xxl_source" == true ]]; then
        # 只在临时副本覆盖订单服务及其内部类；前后均用同一候选依赖编译，不覆盖原候选/target。
        legacy_source="$legacy_repo/cardShop-admin/src/main/java/com/ruoyi/web/mc/service/impl/McOrderServiceImpl.java"
        [[ -f "$legacy_source" ]] || fail '缺旧端订单服务源码'
        "$JAVA8_HOME/bin/javac" -encoding UTF-8 -cp "$COMPAT_LEGACY_CLASSPATH" \
            -d "$scratch/candidate/BOOT-INF/classes" "$legacy_source"
        python3 - "$legacy_source" "$scratch/candidate/BOOT-INF/classes/com/ruoyi/web/mc/service/impl/McOrderServiceImpl.class" <<'PY'
import hashlib, pathlib, sys
for label, path in zip(('service source', 'compiled service'), sys.argv[1:]):
    print('LEGACY ' + label + ' SHA256=' + hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest())
print('LEGACY mode=candidate-dependencies-with-source-compiled-service (not original packaged service)')
PY
    fi
    printf '<configuration><appender name="console" class="ch.qos.logback.core.ConsoleAppender"><encoder><pattern>%%level %%logger - %%msg%%n</pattern></encoder></appender><root level="WARN"><appender-ref ref="console"/></root></configuration>\n' > "$COMPAT_LEGACY_LOGBACK"
    "$JAVA8_HOME/bin/javac" -encoding UTF-8 -cp "$COMPAT_LEGACY_CLASSPATH" -d "$scratch/probe" \
        "$ROOT/scripts/tests/LegacyWriterCompatibilityProbe.java" "$ROOT/scripts/tests/LegacyXxlCompatibilityProbe.java"
    printf 'LEGACY source HEAD=%s (not a deployed-version assertion)\n' "$(git -C "$legacy_repo" rev-parse HEAD)"
fi
export LEGACY_SCHEMA_TEST_PASSWORD="$(openssl rand -hex 24)"
printf 'MYSQL_ROOT_PASSWORD=%s\nMYSQL_ROOT_HOST=%%\n' "$LEGACY_SCHEMA_TEST_PASSWORD" > "$scratch/mysql.env"
container="$(docker run -d --platform "$platform" --pull never --env-file "$scratch/mysql.env" \
    -p 127.0.0.1::3306 "$image" --lower-case-table-names=1 --transaction-isolation=READ-COMMITTED \
    --character-set-server=utf8mb3 --collation-server=utf8mb3_general_ci)"
export LEGACY_SCHEMA_TEST_PORT="$(docker port "$container" 3306/tcp | sed -n 's/^127\.0\.0\.1://p')"
ready=false
for ((attempt=0; attempt<120; attempt++)); do
    if docker exec -e MYSQL_PWD="$LEGACY_SCHEMA_TEST_PASSWORD" "$container" mysql -uroot -Nse 'SELECT 1' >/dev/null 2>&1; then
        ready=true; break
    fi
    sleep 1
done
[[ "$ready" == true ]] || fail 'MySQL启动超时'
printf 'COMPAT fixture image=%s platform=%s localPort=%s\n' "$image" "$platform" "$LEGACY_SCHEMA_TEST_PORT"
run_tests() {
    JAVA_HOME="$JAVA21_HOME" PATH="$JAVA21_HOME/bin:$PATH" mvn -B -f "$ROOT/pom.xml" \
        -pl cardshop-trade-engine-infrastructure -am -Dproduction.compatibility.mysqlTests=true \
        -Dsurefire.failIfNoSpecifiedTests=false -Dtest="$1" "${@:2}" test
}
verify_report() {
    python3 - "$ROOT/$1/target/surefire-reports/TEST-com.cardshop.trade.engine.$2.xml" "$3" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
assert int(r.attrib['tests']) == int(sys.argv[2]), r.attrib
assert all(int(r.attrib.get(k, 0)) == 0 for k in ('failures', 'errors', 'skipped')), r.attrib
print('VERIFIED:', r.attrib['name'], 'tests=' + r.attrib['tests'], 'failures=0 errors=0 skipped=0')
PY
}
if [[ "$legacy_xxl" == true ]]; then
    # 专项独立选择；不修改或运行用户已接受风险的后台编辑/删除断言。
    run_tests 'ProductionLegacyWriterMysqlTest#activeXxlTasksRespectEngineOwnershipAndStillProcessLegacyOrders' \
        -Dproduction.compatibility.legacyWriterTests=true
    verify_report cardshop-trade-engine-infrastructure infrastructure.persistence.ProductionLegacyWriterMysqlTest 1
    printf 'PASS: Java8 active XXL ownership contract, sourceCompiledService=%s, synthetic local data only\n' "$legacy_xxl_source"
    exit 0
fi
run_tests 'ProductionCompatibilityMigrationTest,ProductionOrderIdentityMysqlTest'
verify_report cardshop-trade-engine-migration migration.ProductionCompatibilityMigrationTest 59
verify_report cardshop-trade-engine-infrastructure infrastructure.persistence.ProductionOrderIdentityMysqlTest 23
run_tests 'ProductionOrderIdentityMysqlTest' -Dproduction.compatibility.collation=utf8mb4_0900_ai_ci
verify_report cardshop-trade-engine-infrastructure infrastructure.persistence.ProductionOrderIdentityMysqlTest 23
# 第一个Surefire JVM正常退出后，再启动另一个JVM读取同一份已提交事实，不能用重建对象冒充重启。
export COMPAT_RECOVERY_SCHEMA="compat_identity_$(openssl rand -hex 16)"
run_tests 'ProductionOrderIdentityMysqlTest#commitThenRecoverAcrossJvms' -Dproduction.compatibility.restartStage=facts
verify_report cardshop-trade-engine-infrastructure infrastructure.persistence.ProductionOrderIdentityMysqlTest 1
run_tests 'ProductionOrderIdentityMysqlTest#commitThenRecoverAcrossJvms' -Dproduction.compatibility.restartStage=recover
verify_report cardshop-trade-engine-infrastructure infrastructure.persistence.ProductionOrderIdentityMysqlTest 1
if [[ -n "$legacy_repo" ]]; then
    run_tests 'ProductionLegacyWriterMysqlTest#originalJava8CandidateWritesAndRefundsWithoutBreakingScopedOwnership' \
        -Dproduction.compatibility.legacyWriterTests=true
    verify_report cardshop-trade-engine-infrastructure infrastructure.persistence.ProductionLegacyWriterMysqlTest 1
    printf 'PASS: original Java8 candidate, seven writer/protection cases after real engine migration\n'
else
    printf 'NOT RUN: original Java8 legacy candidate (requires --legacy-repo PATH)\n'
fi
printf 'PASS: migration, identity, concurrency, committed-facts restart recovery; synthetic local data only\n'

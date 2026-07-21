package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * D2 Mapper 契约测试。
 *
 * <p>Mapper 合同对应 {@code docs/交行直充业务系统详细设计.md} 的「8. MySQL/Liquibase 持久化层」
 * 和用户补充要求：SQL 禁止多表关联查询，优先使用 MyBatis-Plus LambdaWrapper。
 */
class MapperContractTest {

    @Test
    void mcOrderMapperUsesLegacyColumnNamesAndFailCompatibilityFields() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/McOrderMapper.java"));

        assertThat(mapper)
                .contains("insert into mc_order")
                .contains("order_Id")
                .contains("out_orderId")
                .contains("produt_code")
                .contains("fail_code")
                .contains("fail_reason")
                .contains("on duplicate key update")
                .contains("order_Id = order_Id")
                .doesNotContainIgnoringCase(" join ");
    }

    @Test
    void rechargeOrderExtMapperProtectsVersionForwardOnlyState() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOrderExtMapper.java"));

        assertThat(mapper)
                .contains("insert into recharge_order_ext")
                .contains("persist_status = case")
                .contains("values(persisted_version) > persisted_version")
                .contains("values(persist_status) in ('DB_PERSISTED', 'MANUAL_REQUIRED')")
                .contains("persisted_version = greatest(persisted_version, values(persisted_version))")
                .contains("redis_order_version = greatest(redis_order_version, values(redis_order_version))");
        assertThat(mapper.indexOf("persist_source = case"))
                .as("persist_source 要在 persist_status 之前赋值，避免 MySQL 顺序求值看到已更新状态")
                .isLessThan(mapper.indexOf("persist_status = case"));
        assertThat(mapper.indexOf("last_error = case"))
                .as("last_error 要在 persisted_version 之前赋值，避免同一 UPDATE 内版本先被 greatest 改写")
                .isLessThan(mapper.indexOf("persisted_version = greatest"));
    }

    @Test
    void rechargeOrderExtMapperHasBatchPersistedStateLookup() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOrderExtMapper.java"));

        assertThat(mapper)
                .contains("List<RechargeOrderExtDO> selectByOrderIds")
                .contains("from recharge_order_ext")
                .contains("where order_id in")
                .contains("<foreach collection=\"orderIds\" item=\"orderId\"")
                .contains("persist_status")
                .contains("persisted_version")
                .doesNotContainIgnoringCase(" join ");
    }

    @Test
    void acceptAnomalyMapperRefreshesDuplicateByStableAnomalyId() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeAcceptAnomalyMapper.java"));

        assertThat(mapper)
                .contains("insert into recharge_accept_anomaly")
                .contains("on duplicate key update")
                .contains("last_error = values(last_error)")
                .contains("updated_at = values(updated_at)");
    }

    @Test
    void acceptAnomalyCheckResultOnlyUpdatesOpenStatuses() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeAcceptAnomalyMapper.java"));
        String updateCheckResult = mapper.substring(mapper.indexOf("default int updateCheckResult"));

        assertThat(updateCheckResult)
                .contains("RechargeAcceptAnomalyDO::getAnomalyId, row.getAnomalyId())")
                .contains("AcceptAnomalyStatus.INIT.name()")
                .contains("AcceptAnomalyStatus.CHECKING.name()")
                .contains("AcceptAnomalyStatus.MANUAL_REQUIRED.name()");
    }

    @Test
    void v1RuntimeReadersUseExplicitSingleTableSelectSql() throws IOException {
        String merchantMapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/McExtDepartMapper.java"));
        String departProductMapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/McDepartProductMapper.java"));
        String productMapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/McProductMapper.java"));

        assertThat(merchantMapper)
                .contains("@Select")
                .contains("from mc_ext_depart")
                .doesNotContain("selectOne(")
                .doesNotContain("LambdaQueryWrapper")
                .doesNotContainIgnoringCase(" join ");
        assertThat(departProductMapper)
                .contains("@Select")
                .contains("from mc_depart_product")
                .contains("and parent_id = #{productId}")
                .doesNotContain("and product_id = #{productId}")
                .doesNotContain("selectList(")
                .doesNotContain("LambdaQueryWrapper")
                .doesNotContainIgnoringCase(" join ");
        assertThat(productMapper)
                .contains("@Select")
                .contains("from mc_product")
                .doesNotContain("selectList(")
                .doesNotContain("LambdaQueryWrapper")
                .doesNotContainIgnoringCase(" join ");
    }

    @Test
    void mcDepartRecordMapperUsesLegacySingleTableColumns() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/McDepartRecordMapper.java"));

        assertThat(mapper)
                .contains("insert into mc_depart_record")
                .contains("before_balance")
                .contains("create_time")
                .contains("type")
                .contains("`value`")
                .contains("on duplicate key update")
                .contains("order_id = order_id")
                .doesNotContain(".last(\"limit 1\")")
                .doesNotContain("record_type")
                .doesNotContain("record_id")
                .doesNotContainIgnoringCase(" join ");
    }

    @Test
    void dispatchOutboxMapperTargetsDispatchTableAndOwnedOrders() throws IOException {
        String mapper = Files.readString(Path.of(
                "src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeDispatchOutboxMapper.java"));
        String entity = Files.readString(Path.of(
                "src/main/java/com/cardshop/trade/engine/infrastructure/persistence/entity/RechargeDispatchOutboxDO.java"));

        assertThat(entity)
                .contains("@TableName(\"recharge_dispatch_outbox\")")
                .contains("「9.1 派单 Outbox」");
        assertThat(mapper)
                .contains("insert into recharge_dispatch_outbox")
                .contains("insertDispatchOutboxBatchIfAbsent")
                .contains("OutboxEventType.SUPPLIER_DISPATCH.name()")
                .contains("OutboxEventType.SUPPLIER_QUERY.name()")
                .contains("OutboxEventType.INVALID_ORDER.name()")
                .contains("from recharge_dispatch_outbox o")
                .contains("from recharge_order_ext ext")
                .contains("ext.order_id = o.order_id")
                .contains("ext.org_code = o.org_code")
                .contains("ext.out_order_sn = o.out_order_sn")
                .contains("ext.persist_status = 'DB_PERSISTED'")
                .contains("'INIT'")
                .contains("'FAILED'")
                .contains("'PROCESSING'")
                .contains("retry_count < max_retry_count")
                .contains("mod(crc32(o.shard_key), #{shardTotal}) = #{shardIndex}")
                .contains("version = version + 1")
                .contains("selectObservabilityDispatchOutboxMetrics")
                .doesNotContain("sumUnfinishedBalanceDelta")
                .doesNotContainIgnoringCase(" join ");
    }

    @Test
    void dispatchOutboxMapperScansEligibleEventsByCreationTime() throws IOException {
        String mapper = Files.readString(Path.of(
                "src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeDispatchOutboxMapper.java"));

        assertThat(mapper)
                .contains("List<RechargeDispatchOutboxDO> selectDueForShardByEventTypesSql")
                .contains("o.next_retry_time &lt;= #{now}")
                .contains("order by o.created_at asc, o.id asc")
                .doesNotContain("orderByAsc(RechargeDispatchOutboxDO::getNextRetryTime)")
                .doesNotContain("order by o.next_retry_time asc");
    }

    @Test
    void balanceDeltaOutboxMapperUsesStructuredDeltaColumnsWithoutPayload() throws IOException {
        String mapper = Files.readString(Path.of(
                "src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeBalanceDeltaOutboxMapper.java"));
        String entity = Files.readString(Path.of(
                "src/main/java/com/cardshop/trade/engine/infrastructure/persistence/entity/RechargeBalanceDeltaOutboxDO.java"));

        assertThat(entity)
                .contains("@TableName(\"recharge_balance_delta_outbox\")")
                .contains("「9.2 余额 Delta 刷账 Outbox」")
                .contains("deltaType")
                .contains("deltaAmount")
                .doesNotContain("payload")
                .doesNotContain("eventType")
                .doesNotContain("shardKey");

        String insertIfAbsent = mapper.substring(mapper.indexOf("int insertIfAbsent"), mapper.indexOf("int insertBalanceDeltaBatchIfAbsent"));
        assertThat(insertIfAbsent)
                .contains("insert into recharge_balance_delta_outbox")
                .contains("delta_type")
                .contains("delta_amount")
                .doesNotContain("payload")
                .doesNotContain("event_type")
                .doesNotContain("shard_key");

        assertThat(mapper)
                .contains("insertBalanceDeltaBatchIfAbsent")
                .contains("selectDueForFlush")
                .contains("mod(crc32(o.event_id), #{shardTotal}) = #{shardIndex}")
                .contains("order by o.created_at asc, o.id asc")
                .contains("Long sumUnfinishedBalanceDelta")
                .contains("coalesce(sum(o.delta_amount), 0)")
                .contains("selectObservabilityBalanceDeltaMetrics")
                .contains("group by o.status")
                .contains("from recharge_order_ext ext")
                .contains("ext.persist_status = 'DB_PERSISTED'")
                .doesNotContain("group by o.event_type")
                .doesNotContainIgnoringCase(" join ");
    }

    @Test
    void notifyTaskMapperAndAdapterReclaimExpiredProcessingTasks() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeNotifyTaskMapper.java"));
        String selectDueTasks = mapper.substring(mapper.indexOf("default List<RechargeNotifyTaskDO> selectDueTasks"));
        assertThat(selectDueTasks)
                .contains("TaskStatus.PROCESSING.name()")
                .contains("RechargeNotifyTaskDO::getLockedUntil")
                .contains("isNotNull(RechargeNotifyTaskDO::getNotifyUrl)")
                .contains("ne(RechargeNotifyTaskDO::getNotifyUrl, \"\")");

        String adapter = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeNotifyTaskAdapter.java"));
        String claim = adapter.substring(adapter.indexOf("public Optional<RechargeNotifyTask> claim"));
        assertThat(claim)
                .contains("TaskStatus.PROCESSING.name()")
                .contains("RechargeNotifyTaskDO::getLockedUntil");
        assertThat(adapter)
                .contains("public int closeBlankNotifyUrlTasks()")
                .contains("TaskStatus.MANUAL_REQUIRED.name()");
    }

    @Test
    void runtimeUsesMybatisPlusBoot3StarterForBaseMapperSqlInjector() throws IOException {
        String rootPom = Files.readString(Path.of("../pom.xml").normalize());
        String infrastructurePom = Files.readString(Path.of("pom.xml"));

        assertThat(rootPom)
                .contains("<mybatis-plus.version>3.5.4.1</mybatis-plus.version>")
                .contains("<mybatis-spring.version>3.0.3</mybatis-spring.version>")
                .contains("<artifactId>mybatis-plus-spring-boot3-starter</artifactId>")
                .contains("<artifactId>mybatis-spring</artifactId>")
                .contains("<version>${mybatis-spring.version}</version>")
                .doesNotContain("<artifactId>mybatis-plus-core</artifactId>");
        assertThat(infrastructurePom)
                .contains("<artifactId>mybatis-plus-spring-boot3-starter</artifactId>")
                .doesNotContain("<artifactId>mybatis-spring-boot-starter</artifactId>")
                .doesNotContain("<artifactId>mybatis-plus-core</artifactId>");
    }

    @Test
    void persistenceCodeDoesNotUseMultiTableJoinQueries() throws IOException {
        Path root = Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence");

        try (var files = Files.walk(root)) {
            var joinedSource = files
                    .filter(path -> path.toString().endsWith(".java"))
                    .map(path -> {
                        try {
                            return Files.readString(path);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    })
                    .reduce("", (left, right) -> left + "\n" + right);

            assertThat(joinedSource)
                    .contains("LambdaQueryWrapper")
                    .doesNotContainIgnoringCase(" join ")
                    .doesNotContainIgnoringCase(" left join ")
                    .doesNotContainIgnoringCase(" right join ")
                    .doesNotContainIgnoringCase(" inner join ");
        }
    }
}

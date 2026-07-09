package com.cardshop.trade.engine.infrastructure.config;

import com.baomidou.mybatisplus.autoconfigure.DdlApplicationRunner;
import com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration;
import com.baomidou.mybatisplus.extension.ddl.IDdl;
import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * MyBatis-Plus DDL Runner 的 Spring Boot 3.3 兼容配置。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8. Liquibase 数据库版本控制」和
 * 「20.7 MyBatis Mapper 细节」。本服务的表结构变更统一由 Liquibase 管理，MyBatis-Plus 只用于
 * {@code BaseMapper + LambdaWrapper} 的运行期 SQL 注入，不承担建表或变更表结构职责。
 *
 * <p>MyBatis-Plus 3.5.4.1 的 {@link MybatisPlusAutoConfiguration#ddlApplicationRunner(List)}
 * 在没有任何 {@link IDdl} 脚本 Bean 时会从 {@code @Bean} 方法返回 {@code null}。Spring 会把该结果注册成
 * {@code NullBean}，而 Spring Boot 3.3 在 {@code SpringApplication.callRunners} 阶段会按 {@code Runner}
 * 收集并调用它，真实 dev/test 启动因此失败。这里在 MyBatis-Plus 自动配置之前提供一个空脚本列表的真实
 * {@link DdlApplicationRunner}；该 Runner 的 {@code run} 对空列表是 no-op，既避免 {@code NullBean}，
 * 也不会访问数据库或绕过 Liquibase 的 DDL 管控。
 */
@AutoConfiguration(before = MybatisPlusAutoConfiguration.class)
@ConditionalOnClass({MybatisPlusAutoConfiguration.class, DdlApplicationRunner.class})
public class MybatisPlusDdlRunnerCompatibilityAutoConfiguration {

    /**
     * 在未显式声明 MyBatis-Plus {@link IDdl} 脚本时，提前占位 {@code ddlApplicationRunner}。
     *
     * <p>方法名保持为 MyBatis-Plus 原始 Bean 名称，便于启动日志、问题排查和测试断言对齐。若未来确需使用
     * MyBatis-Plus DDL 脚本，只要声明 {@link IDdl} 或自定义 {@link DdlApplicationRunner}，本兼容 Bean 就会让位；
     * 当前 dev/test/prod 仍应继续以 Liquibase 作为唯一 DDL 执行入口。
     */
    @Bean
    @ConditionalOnMissingBean({DdlApplicationRunner.class, IDdl.class})
    public DdlApplicationRunner ddlApplicationRunner() {
        return new DdlApplicationRunner(List.of());
    }
}

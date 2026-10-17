package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.recharge.MinisoWriteOffStore;
import com.cardshop.trade.engine.infrastructure.config.MinisoWriteOffProperties;
import com.cardshop.trade.engine.job.recharge.MinisoWriteOffProcessJob;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;

/** 独立恢复开关，只依赖已认证持久事件，不要求入站key2；参见docs/交行直充业务系统详细设计.md「9.5」。 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
        prefix = "trade.engine.supplier.miniso.callback",
        name = "processing-enabled",
        havingValue = "true")
public class MinisoWriteOffJobConfiguration {
    @Bean
    public MinisoWriteOffProcessJob minisoWriteOffProcessJob(MinisoWriteOffStore store) {
        return new MinisoWriteOffProcessJob(store);
    }

    @Bean
    public MinisoWriteOffXxlHandler minisoWriteOffXxlHandler(
            MinisoWriteOffProcessJob job, MinisoWriteOffProperties settings) {
        return new MinisoWriteOffXxlHandler(job, settings.getBatchSize());
    }
}

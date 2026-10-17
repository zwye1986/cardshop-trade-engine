package com.cardshop.trade.engine.infrastructure.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** 名创回调独立配置，不随主动发券开关停恢复；参见docs/交行直充业务系统详细设计.md「9.5」。 */
@ConfigurationProperties(prefix = "trade.engine.supplier.miniso.callback")
public class MinisoWriteOffProperties implements InitializingBean {
    private boolean enabled = false;
    private boolean processingEnabled = false;
    private String key2 = "";
    private int batchSize = 100;
    private int retryBackoffSeconds = 60;
    private int maxRetryCount = 100;
    private int maxBodyBytes = 4096;
    private int receiveThreads = 4;
    private int receiveQueueCapacity = 256;
    private int transactionTimeoutSeconds = 5;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean value) {
        enabled = value;
    }

    public boolean isProcessingEnabled() {
        return processingEnabled;
    }

    public void setProcessingEnabled(boolean value) {
        processingEnabled = value;
    }

    public String getKey2() {
        return key2;
    }

    public void setKey2(String value) {
        key2 = value;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int value) {
        batchSize = value;
    }

    public int getRetryBackoffSeconds() {
        return retryBackoffSeconds;
    }

    public void setRetryBackoffSeconds(int value) {
        retryBackoffSeconds = value;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int value) {
        maxRetryCount = value;
    }

    public int getMaxBodyBytes() {
        return maxBodyBytes;
    }

    public void setMaxBodyBytes(int value) {
        maxBodyBytes = value;
    }

    public int getReceiveThreads() {
        return receiveThreads;
    }

    public void setReceiveThreads(int value) {
        receiveThreads = value;
    }

    public int getReceiveQueueCapacity() {
        return receiveQueueCapacity;
    }

    public void setReceiveQueueCapacity(int value) {
        receiveQueueCapacity = value;
    }

    public int getTransactionTimeoutSeconds() {
        return transactionTimeoutSeconds;
    }

    public void setTransactionTimeoutSeconds(int value) {
        transactionTimeoutSeconds = value;
    }

    @Override
    public void afterPropertiesSet() {
        if (enabled && (!processingEnabled || key2 == null || key2.isBlank()))
            throw new IllegalArgumentException(
                    "MINISO reception requires processing-enabled and independent key2");
        if (batchSize < 1
                || batchSize > 1000
                || retryBackoffSeconds < 1
                || maxRetryCount < 1
                || maxBodyBytes < 1
                || maxBodyBytes > 4096
                || receiveThreads < 1
                || receiveThreads > 64
                || receiveQueueCapacity < 1
                || transactionTimeoutSeconds < 1
                || transactionTimeoutSeconds > 5)
            throw new IllegalArgumentException("Invalid MINISO callback resource bounds");
    }

    @Override
    public String toString() {
        return "MinisoWriteOffProperties[enabled=" + enabled + ",key2=REDACTED]";
    }
}

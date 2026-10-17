package com.cardshop.trade.engine.api.miniso;

import java.time.Duration;

/** 入站资源上限视图，API不读取基础设施配置；参见docs/交行直充业务系统详细设计.md「9.5」。 */
public record MinisoWriteOffEndpointSettings(int maxBodyBytes, Duration timeout) {}

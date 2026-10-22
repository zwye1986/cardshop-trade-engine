package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.bocom.api.AbstractBocomRequest;
import com.bocom.api.BizContent;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 交行 2104 官方 SDK 请求：只映射官方 head/body 包装，不在此拼接业务内容或签名。
 * 参见 docs/交行直充业务系统详细设计.md「交行 2104 通知实施细化」。
 */
public final class Bocom2104Request extends AbstractBocomRequest<Bocom2104Response> {
    @Override public Class<Bocom2104Response> getResponseClass() { return Bocom2104Response.class; }
    @Override public boolean isNeedEncrypt() { return false; }
    @Override public String getMethod() { return "POST"; }
    @Override public Class<? extends BizContent> getBizContentClass() { return Content.class; }

    /** 官方请求所有信息包装。 */
    public static final class Content implements BizContent {
        @JsonProperty("head") private Head head;
        @JsonProperty("body") private Body body;
        public Head getHead() { return head; }
        public void setHead(Head head) { this.head = head; }
        public Body getBody() { return body; }
        public void setBody(Body body) { this.body = body; }
    }

    /** 官方请求头当前没有业务字段，须发送空对象。 */
    // 新版 Jackson 默认拒绝无属性 bean；显式序列化注解保证官方空头输出为 {}。
    @com.fasterxml.jackson.databind.annotation.JsonSerialize
    public static final class Head { }

    /** 官方请求体；业务字段含义及顺序见详细设计「交行 2104 通知实施细化」。 */
    public static final class Body {
        /** 请求所有信息，使用已持久化的 trx_data_imprt 原文。 */
        @JsonProperty("trx_data_imprt") private String trxDataImprt;
        /** 协议信息。 */
        @JsonProperty("protocol") private String protocol;
        public String getTrxDataImprt() { return trxDataImprt; }
        public void setTrxDataImprt(String trxDataImprt) { this.trxDataImprt = trxDataImprt; }
        public String getProtocol() { return protocol; }
        public void setProtocol(String protocol) { this.protocol = protocol; }
    }
}

package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.bocom.api.BocomResponse;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 官方 SDK 验签后得到的 2104 响应，网关成功与 body.code=200 是两个独立判定。
 * 参见 docs/交行直充业务系统详细设计.md「交行 2104 通知实施细化」。
 */
public final class Bocom2104Response extends BocomResponse {
    /** HTTP状态码，来自受控传输，不属于银行业务JSON。 */
    @com.fasterxml.jackson.annotation.JsonIgnore
    private int transportHttpStatus;
    public int getTransportHttpStatus() { return transportHttpStatus; }
    public void setTransportHttpStatus(int value) { transportHttpStatus=value; }
    @JsonProperty("head") private Head head;
    @JsonProperty("body") private Body body;
    public Head getHead() { return head; }
    public void setHead(Head head) { this.head = head; }
    public Body getBody() { return body; }
    public void setBody(Body body) { this.body = body; }

    /** 官方响应头当前没有业务字段。 */
    public static final class Head { }

    /** 官方响应体。 */
    public static final class Body {
        /** 200：成功。 */
        @JsonProperty("code") private String code;
        /** 返回信息。 */
        @JsonProperty("msg") private String msg;
        public String getCode() { return code; }
        public void setCode(String code) { this.code = code; }
        public String getMsg() { return msg; }
        public void setMsg(String msg) { this.msg = msg; }
    }
}

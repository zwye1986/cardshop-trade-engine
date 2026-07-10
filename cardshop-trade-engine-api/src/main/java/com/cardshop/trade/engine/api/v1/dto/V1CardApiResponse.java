package com.cardshop.trade.engine.api.v1.dto;

/**
 * `/v1/cardapi` 统一响应包装。
 *
 * <p>响应形状对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」：
 * 对外保持 {@code code/msg/data} 三段式结构，内部失败码不得替换旧接口数字状态码。
 *
 * @param code 旧接口数字状态码字符串，例如 200、500、505
 * @param msg 旧接口响应文案
 * @param data 响应数据对象
 */
public record V1CardApiResponse<T>(String code, String msg, T data) {

    /**
     * 构造旧接口成功响应。
     */
    public static <T> V1CardApiResponse<T> success(T data) {
        return new V1CardApiResponse<>("200", "操作成功", data);
    }

    /**
     * 构造旧接口失败响应。
     */
    public static <T> V1CardApiResponse<T> failure(String code, String msg, T data) {
        return new V1CardApiResponse<>(code, msg, data);
    }
}

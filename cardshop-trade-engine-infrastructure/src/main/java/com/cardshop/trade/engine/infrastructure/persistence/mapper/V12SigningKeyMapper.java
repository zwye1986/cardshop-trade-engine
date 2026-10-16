package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import java.util.Map;
import org.apache.ibatis.annotations.*;

/** 独立签名密钥表Mapper；对应详细设计「V1.2 密钥存储实施约束」。所有查询按商户隔离。 */
@Mapper
public interface V12SigningKeyMapper {
    @Insert("""
        INSERT INTO recharge_v12_signing_key(key_id,app_id,org_code,encrypted_sign_key,request_expires_at_ms)
        VALUES(#{keyId},#{appId},#{orgCode},#{ciphertext},#{expiresAt})
        """)
    int insert(@Param("keyId") String keyId, @Param("appId") String appId,
               @Param("orgCode") String orgCode, @Param("ciphertext") String ciphertext,
               @Param("expiresAt") long expiresAt);

    @Select("""
        SELECT key_id,app_id,org_code,encrypted_sign_key,request_expires_at_ms
        FROM recharge_v12_signing_key
        WHERE key_id=#{keyId} AND app_id=#{appId} AND org_code=#{orgCode} AND revoked_at IS NULL
        """)
    Map<String,Object> findActive(@Param("appId") String appId, @Param("orgCode") String orgCode,
                                 @Param("keyId") String keyId);

    @Select("SELECT signing_key_id FROM recharge_order_ext WHERE org_code=#{orgCode} AND order_id=#{orderId}")
    String findOrderKeyId(@Param("orgCode") String orgCode, @Param("orderId") String orderId);

    @Select("""
        SELECT key_id,app_id,org_code,encrypted_sign_key,request_expires_at_ms
        FROM recharge_v12_signing_key
        WHERE org_code=#{orgCode} AND key_id=#{keyId} AND revoked_at IS NULL
        """)
    Map<String,Object> findActiveForOrg(@Param("orgCode") String orgCode, @Param("keyId") String keyId);

    @Update("""
        UPDATE recharge_v12_signing_key SET revoked_at=CURRENT_TIMESTAMP(3)
        WHERE org_code=#{orgCode} AND key_id=#{keyId} AND revoked_at IS NULL
        """)
    int revoke(@Param("orgCode") String orgCode, @Param("keyId") String keyId);
}

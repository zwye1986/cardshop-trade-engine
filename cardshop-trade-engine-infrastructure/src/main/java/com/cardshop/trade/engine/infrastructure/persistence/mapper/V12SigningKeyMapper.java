package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import java.util.Map;
import org.apache.ibatis.annotations.*;

/**
 * 商户当前指针和签名密钥版本的持久化；参见详细设计「V1.2 密钥存储实施约束」。
 * 所有读写均带可信商户编码；初始化、轮换、撤销统一先锁定商户指针行。
 */
@Mapper
public interface V12SigningKeyMapper {
    @Insert("""
        INSERT INTO recharge_v12_merchant_key(org_code) VALUES(#{orgCode})
        ON DUPLICATE KEY UPDATE org_code=VALUES(org_code)
        """)
    int ensureMerchant(@Param("orgCode") String orgCode);

    @Select("SELECT current_key_id FROM recharge_v12_merchant_key WHERE org_code=#{orgCode} FOR UPDATE")
    String lockCurrentKeyId(@Param("orgCode") String orgCode);

    @Select("SELECT current_key_id FROM recharge_v12_merchant_key WHERE org_code=#{orgCode}")
    String currentKeyId(@Param("orgCode") String orgCode);

    @Insert("""
        INSERT INTO recharge_v12_merchant_signing_key(key_id,org_code,encrypted_sign_key,key_status)
        VALUES(#{keyId},#{orgCode},#{ciphertext},'CURRENT')
        """)
    int insertVersion(@Param("keyId") String keyId, @Param("orgCode") String orgCode,
                      @Param("ciphertext") String ciphertext);

    @Update("UPDATE recharge_v12_merchant_key SET current_key_id=#{keyId} WHERE org_code=#{orgCode}")
    int setCurrent(@Param("orgCode") String orgCode, @Param("keyId") String keyId);

    @Select("""
        SELECT key_id,org_code,encrypted_sign_key FROM recharge_v12_merchant_signing_key
        WHERE org_code=#{orgCode} AND key_id=#{keyId} AND key_status='CURRENT' AND revoked_at IS NULL
        """)
    Map<String, Object> findCurrentVersion(@Param("orgCode") String orgCode, @Param("keyId") String keyId);

    @Select("""
        SELECT key_id,org_code,encrypted_sign_key FROM recharge_v12_merchant_signing_key
        WHERE org_code=#{orgCode} AND key_id=#{keyId} AND key_status IN ('CURRENT','RETIRED') AND revoked_at IS NULL
        """)
    Map<String, Object> findCallbackVersion(@Param("orgCode") String orgCode, @Param("keyId") String keyId);

    @Update("""
        UPDATE recharge_v12_merchant_signing_key SET key_status='RETIRED',retired_at=CURRENT_TIMESTAMP(3)
        WHERE org_code=#{orgCode} AND key_id=#{keyId} AND key_status='CURRENT' AND revoked_at IS NULL
        """)
    int retire(@Param("orgCode") String orgCode, @Param("keyId") String keyId);

    @Update("""
        UPDATE recharge_v12_merchant_signing_key SET key_status='REVOKED',revoked_at=CURRENT_TIMESTAMP(3)
        WHERE org_code=#{orgCode} AND key_id=#{keyId} AND key_status IN ('CURRENT','RETIRED') AND revoked_at IS NULL
        """)
    int revoke(@Param("orgCode") String orgCode, @Param("keyId") String keyId);
}

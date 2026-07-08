package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.cardshop.trade.engine.infrastructure.persistence.V1MerchantProfileReader;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 旧卡管商户扩展表 `mc_ext_depart` Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」。
 * 当前部署包未启用 MyBatis-Plus 通用 SQL 注入，因此 V1 运行期 Reader 使用显式单表
 * {@code @Select}，避免依赖 {@code BaseMapper.selectOne/selectList} 在运行期缺少 mapped statement。
 */
@Mapper
public interface McExtDepartMapper extends V1MerchantProfileReader {

    /**
     * 按 appId 和可选 orgCode 读取商户资料；旧 token 可能只携带 appId，因此 orgCode 为空时不追加机构条件。
     */
    @Override
    @Select("""
            <script>
            select
              id,
              org_code as orgCode,
              app_id as appId,
              app_secret as appSecret,
              aes_key as aesKey,
              depart_status as departStatus
            from mc_ext_depart
            where app_id = #{appId}
            <if test="orgCode != null and orgCode != ''">
              and org_code = #{orgCode}
            </if>
            order by id asc
            limit 1
            </script>
            """)
    McExtDepartDO selectByAppIdAndOptionalOrg(
            @Param("appId") String appId,
            @Param("orgCode") String orgCode);

    /**
     * 按旧系统 `/auth/getToken` 凭证语义读取商户资料；启停状态交由应用用例统一转换为 401。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.5 Token 生成接口」。
     * 这里不能追加 {@code limit 1}：appId/appSecret 代表签发凭证，若旧表出现重复数据，应让 MyBatis
     * 对单对象返回的非唯一结果抛出异常，再由适配器失败关闭，避免把 token 签发到任意一条商户记录上。
     */
    @Override
    @Select("""
            select
              id,
              org_code as orgCode,
              app_id as appId,
              app_secret as appSecret,
              aes_key as aesKey,
              depart_status as departStatus
            from mc_ext_depart
            where app_id = #{appId}
              and app_secret = #{appSecret}
            """)
    McExtDepartDO selectByAppIdAndAppSecret(
            @Param("appId") String appId,
            @Param("appSecret") String appSecret);

    @Override
    @Select("""
            select
              id,
              org_code as orgCode,
              app_id as appId,
              app_secret as appSecret,
              aes_key as aesKey,
              depart_status as departStatus
            from mc_ext_depart
            where org_code = #{orgCode}
            order by id asc
            limit 1
            """)
    McExtDepartDO selectByOrgCode(@Param("orgCode") String orgCode);
}

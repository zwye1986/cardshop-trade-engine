package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.cardshop.trade.engine.infrastructure.persistence.V1MerchantProfileReader;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

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

    /**
     * 按商户维度扫描余额修复/对账候选行。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.4 缓存修复任务」和「17.5 余额对账任务」。
     * 分片字段使用 {@code org_code}，只返回重建 Redis 余额基线所需的最小字段，避免把 app_secret/aes_key
     * 等密钥字段带入后台日志或任务上下文。
     */
    @Select("""
            select
              id,
              org_code as orgCode,
              coalesce(balance, 0) as balance
            from mc_ext_depart
            where org_code is not null
              and org_code <> ''
              and mod(crc32(org_code), #{shardTotal}) = #{shardIndex}
            order by org_code asc
            limit #{limit}
            """)
    List<McExtDepartDO> selectBalanceRowsForShard(
            @Param("shardTotal") int shardTotal,
            @Param("shardIndex") int shardIndex,
            @Param("limit") int limit);

    /**
     * 原子增量更新商户最终余额。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.7 `mc_ext_depart.balance` 刷账协议」。
     * 下单实时扣减发生在 Redis，MySQL 余额只由 D12 `BALANCE_DELTA_FLUSH` 任务按 delta 聚合沉淀；
     * SQL 使用 `balance = balance + delta`，避免读取旧余额后再覆盖导致并发刷账丢失增量。
     */
    @Update("""
            update mc_ext_depart
            set balance = coalesce(balance, 0) + #{deltaAmount}
            where org_code = #{orgCode}
            """)
    int incrementBalance(
            @Param("orgCode") String orgCode,
            @Param("deltaAmount") long deltaAmount);
}

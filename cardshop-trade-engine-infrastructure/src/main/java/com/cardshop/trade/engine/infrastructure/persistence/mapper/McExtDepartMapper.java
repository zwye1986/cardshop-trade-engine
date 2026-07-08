package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.infrastructure.persistence.V1MerchantProfileReader;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.util.StringUtils;

/**
 * 旧卡管商户扩展表 `mc_ext_depart` Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」。
 * 查询优先使用 MyBatis-Plus LambdaWrapper，并保持单表查询，避免在接口链路引入多表关联。
 */
@Mapper
public interface McExtDepartMapper extends BaseMapper<McExtDepartDO>, V1MerchantProfileReader {

    /**
     * 按 appId 和可选 orgCode 读取商户资料；旧 token 可能只携带 appId，因此 orgCode 为空时不追加机构条件。
     */
    default McExtDepartDO selectByAppIdAndOptionalOrg(String appId, String orgCode) {
        LambdaQueryWrapper<McExtDepartDO> wrapper = new LambdaQueryWrapper<McExtDepartDO>()
                .eq(McExtDepartDO::getAppId, appId);
        if (StringUtils.hasText(orgCode)) {
            wrapper.eq(McExtDepartDO::getOrgCode, orgCode);
        }
        return selectOne(wrapper.last("limit 1"));
    }

    /**
     * 按旧系统 `/auth/getToken` 凭证语义读取商户资料；启停状态交由应用用例统一转换为 401。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.5 Token 生成接口」。
     * 这里不能追加 {@code limit 1}：appId/appSecret 代表签发凭证，若旧表出现重复数据，应让
     * MyBatis-Plus 的 {@code selectOne} 暴露非唯一问题，再由适配器失败关闭，避免把 token 签发到
     * 任意一条商户记录上。
     */
    @Override
    default McExtDepartDO selectByAppIdAndAppSecret(String appId, String appSecret) {
        return selectOne(new LambdaQueryWrapper<McExtDepartDO>()
                .eq(McExtDepartDO::getAppId, appId)
                .eq(McExtDepartDO::getAppSecret, appSecret));
    }

    @Override
    default McExtDepartDO selectByOrgCode(String orgCode) {
        return selectOne(new LambdaQueryWrapper<McExtDepartDO>()
                .eq(McExtDepartDO::getOrgCode, orgCode)
                .last("limit 1"));
    }
}

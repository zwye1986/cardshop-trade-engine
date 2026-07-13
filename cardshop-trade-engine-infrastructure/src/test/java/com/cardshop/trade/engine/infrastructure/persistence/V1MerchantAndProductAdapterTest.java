package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartProductDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McProductDO;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V1 商户资料和商品候选基础设施适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」
 * 和「5. Domain 状态与业务规则」。适配器必须按旧卡管表语义读取数据，但 SQL 只能使用单表
 * MyBatis-Plus LambdaWrapper，再在 Java 内完成商品权限和商品快照组装。
 */
class V1MerchantAndProductAdapterTest {

    @Test
    void loadsMerchantProfileByAppIdAndOrgCode() {
        McExtDepartDO merchant = new McExtDepartDO();
        merchant.setAppId("app-gray");
        merchant.setAppSecret("secret-1");
        merchant.setOrgCode("A36");
        merchant.setAesKey("AES-KEY");
        merchant.setDepartStatus("0");
        merchant.setId(7L);

        MybatisV1MerchantProfileAdapter adapter = new MybatisV1MerchantProfileAdapter((appId, orgCode) -> merchant);

        var profile = adapter.findByAppIdAndOrgCode("app-gray", "A36");

        assertThat(profile).isPresent();
        assertThat(profile.get().aesKey()).isEqualTo("AES-KEY");
        assertThat(profile.get().enabled()).isTrue();
        assertThat(profile.get().configVersion()).isEqualTo(7L);
    }

    @Test
    void loadsMerchantProfileByAppIdAndAppSecretForTokenIssue() {
        McExtDepartDO merchant = new McExtDepartDO();
        merchant.setAppId("app-gray");
        merchant.setAppSecret("secret-1");
        merchant.setOrgCode("A36");
        merchant.setAesKey("AES-KEY");
        merchant.setDepartStatus("0");
        merchant.setId(7L);

        MybatisV1MerchantProfileAdapter adapter = new MybatisV1MerchantProfileAdapter(new V1MerchantProfileReader() {
            @Override
            public McExtDepartDO selectByAppIdAndOptionalOrg(String appId, String orgCode) {
                return null;
            }

            @Override
            public McExtDepartDO selectByAppIdAndAppSecret(String appId, String appSecret) {
                return "app-gray".equals(appId) && "secret-1".equals(appSecret) ? merchant : null;
            }
        });

        var profile = adapter.findByAppIdAndAppSecret("app-gray", "secret-1");

        assertThat(profile).isPresent();
        assertThat(profile.get().appId()).isEqualTo("app-gray");
        assertThat(profile.get().orgCode()).isEqualTo("A36");
        assertThat(profile.get().enabled()).isTrue();
    }

    @Test
    void returnsEmptyWhenCredentialLookupMatchesMoreThanOneMerchant() {
        MybatisV1MerchantProfileAdapter adapter = new MybatisV1MerchantProfileAdapter(new V1MerchantProfileReader() {
            @Override
            public McExtDepartDO selectByAppIdAndOptionalOrg(String appId, String orgCode) {
                return null;
            }

            @Override
            public McExtDepartDO selectByAppIdAndAppSecret(String appId, String appSecret) {
                throw new TooManyResultsException("duplicate app_id/app_secret");
            }
        });

        var profile = adapter.findByAppIdAndAppSecret("app-gray", "secret-1");

        assertThat(profile).isEmpty();
    }

    @Test
    void selectsRequestedProductIdInsteadOfSiblingCandidates() {
        V1DepartProductReader departProductReader = (orgCode, productId) -> List.of(
                departProduct(1L, "73301290", 5200L, 10L),
                departProduct(2L, "73301291", 5200L, 99L));
        V1ProductReader productReader = productIds -> List.of(
                product("73301290", "TF_KA", "话费50元B", 1, "codekey", 5000L, 4900L, 11L),
                product("73301291", "stock", "话费50元A", 1, "codekey", 5000L, 4900L, 12L));

        MybatisV1ProductCandidateSelector selector = new MybatisV1ProductCandidateSelector(
                departProductReader,
                productReader);

        V1ProductSelection selection = selector.select("A36", "73301290", "18186222336");

        assertThat(selection.validationStatus()).isEqualTo(ValidationStatus.PASSED);
        assertThat(selection.productId()).isEqualTo("73301290");
        assertThat(selection.selectedProductId()).isEqualTo("73301290");
        assertThat(selection.supplierChannel()).isEqualTo("TF_KA");
        assertThat(selection.salePriceCent()).isEqualTo(5200L);
        assertThat(selection.productName()).isEqualTo("话费50元B");
        assertThat(selection.productConfigVersion()).isEqualTo(11L);
    }

    @Test
    void failsRequestedProductWhenItIsNotRechargeEvenIfSiblingIsRecharge() {
        V1DepartProductReader departProductReader = (orgCode, productId) -> List.of(
                departProduct(1L, "73301290", 5200L, 10L),
                departProduct(2L, "73301291", 5200L, 99L));
        V1ProductReader productReader = productIds -> List.of(
                product("73301290", "TF_KA", "卡券50元B", 0, "code", 5000L, 4900L, 11L),
                product("73301291", "stock", "话费50元A", 1, "codekey", 5000L, 4900L, 12L));

        MybatisV1ProductCandidateSelector selector = new MybatisV1ProductCandidateSelector(
                departProductReader,
                productReader);

        V1ProductSelection selection = selector.select("A36", "73301290", "18186222336");

        assertThat(selection.validationStatus()).isEqualTo(ValidationStatus.FAILED);
        assertThat(selection.failCode()).isEqualTo("NOT_RECHARGE_PRODUCT");
        assertThat(selection.selectedProductId()).isEqualTo("73301290");
        assertThat(selection.supplierChannel()).isEqualTo("TF_KA");
        assertThat(selection.productType()).isZero();
        assertThat(selection.productConfigVersion()).isEqualTo(11L);
    }

    @Test
    void failsRequestedProductWhenPriceIsBelowBaseEvenIfSiblingIsHealthy() {
        V1DepartProductReader departProductReader = (orgCode, productId) -> List.of(
                departProduct(1L, "73301290", 4800L, 10L),
                departProduct(2L, "73301291", 5200L, 99L));
        V1ProductReader productReader = productIds -> List.of(
                product("73301290", "TF_KA", "话费50元低价商品", 1, "codekey", 5000L, 4900L, 11L),
                product("73301291", "stock", "话费50元库存商品", 1, "codekey", 5000L, 4900L, 12L));

        MybatisV1ProductCandidateSelector selector = new MybatisV1ProductCandidateSelector(
                departProductReader,
                productReader);

        V1ProductSelection selection = selector.select("A36", "73301290", "18186222336");

        assertThat(selection.validationStatus()).isEqualTo(ValidationStatus.FAILED);
        assertThat(selection.failCode()).isEqualTo("PRICE_INVALID");
        assertThat(selection.selectedProductId()).isEqualTo("73301290");
        assertThat(selection.supplierChannel()).isEqualTo("TF_KA");
        assertThat(selection.salePriceCent()).isEqualTo(4800L);
        assertThat(selection.basePriceCent()).isEqualTo(4900L);
        assertThat(selection.productConfigVersion()).isEqualTo(11L);
    }

    @Test
    void returnsProductNoPermissionWhenExactProductIsNotInAuthorizedRows() {
        V1DepartProductReader departProductReader = (orgCode, productId) -> List.of(
                departProduct(2L, "73301291", 5200L, 99L));
        V1ProductReader productReader = productIds -> List.of(
                product("73301291", "stock", "话费50元库存商品", 1, "codekey", 5000L, 4900L, 12L));

        MybatisV1ProductCandidateSelector selector = new MybatisV1ProductCandidateSelector(
                departProductReader,
                productReader);

        V1ProductSelection selection = selector.select("A36", "73301290", "18186222336");

        assertThat(selection.validationStatus()).isEqualTo(ValidationStatus.FAILED);
        assertThat(selection.failCode()).isEqualTo("PRODUCT_NO_PERMISSION");
        assertThat(selection.selectedProductId()).isEqualTo("73301290");
    }

    @Test
    void returnsProductNoPermissionWhenNoEnabledDepartProductExists() {
        MybatisV1ProductCandidateSelector selector = new MybatisV1ProductCandidateSelector(
                (orgCode, productId) -> List.of(),
                productIds -> List.of());

        V1ProductSelection selection = selector.select("A36", "73301280", "18186222336");

        assertThat(selection.validationStatus()).isEqualTo(ValidationStatus.FAILED);
        assertThat(selection.failCode()).isEqualTo("PRODUCT_NO_PERMISSION");
        assertThat(selection.failReason()).isEqualTo("商品无权限");
    }

    private static McDepartProductDO departProduct(Long id, String productId, Long price, Long sort) {
        McDepartProductDO departProduct = new McDepartProductDO();
        departProduct.setId(id);
        departProduct.setOrgCode("A36");
        departProduct.setParentId("73301280");
        departProduct.setProductId(productId);
        departProduct.setPrice(price);
        departProduct.setSort(sort);
        departProduct.setStatus("1");
        return departProduct;
    }

    private static McProductDO product(
            String productId,
            String channel,
            String productName,
            Integer type,
            String couponType,
            Long value,
            Long basePrice,
            Long version) {
        McProductDO product = new McProductDO();
        product.setProductId(productId);
        product.setChannel(channel);
        product.setProductName(productName);
        product.setType(type);
        product.setCouponType(couponType);
        product.setValue(value);
        product.setBasePrice(basePrice);
        product.setVersion(version);
        product.setStatus("1");
        return product;
    }
}

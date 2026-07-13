package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartProductDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McProductDO;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * 旧卡管派单候选顺序测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。旧系统
 * `listByProductIdDIY` 会把订单原商品放在第一位，然后 stock 渠道优先，再按 sort 倒序；已经失败的
 * `mc_order_supplier_flow.channel` 会整体排除。
 */
class LegacyRechargeFulfillmentCandidateSelectorTest {

    @Test
    void ordersOriginalProductFirstThenStockThenSortLikeLegacyListByProductIdDiy() {
        LegacyRechargeFulfillmentCandidateSelector selector = new LegacyRechargeFulfillmentCandidateSelector(
                (orgCode, parentProductId) -> List.of(
                        departProduct(1L, "SKU-OTHER-HIGH", 5200L, 99L),
                        departProduct(2L, "SKU-ORIGINAL", 5200L, 1L),
                        departProduct(3L, "SKU-STOCK", 5200L, 5L),
                        departProduct(4L, "SKU-OTHER-LOW", 5200L, 10L)),
                productIds -> List.of(
                        product("SKU-OTHER-HIGH", "TF_KA", "高 sort 非库存"),
                        product("SKU-ORIGINAL", "FNK_CGS_BX", "订单原渠道"),
                        product("SKU-STOCK", "stock", "库存渠道"),
                        product("SKU-OTHER-LOW", "SZ_CARD", "低 sort 非库存")));

        var candidates = selector.select("A36", "PARENT-1", "SKU-ORIGINAL", Set.of());

        assertThat(candidates)
                .extracting(candidate -> candidate.selectedProductId() + ":" + candidate.supplierChannel())
                .containsExactly(
                        "SKU-ORIGINAL:FNK_CGS_BX",
                        "SKU-STOCK:stock",
                        "SKU-OTHER-HIGH:TF_KA",
                        "SKU-OTHER-LOW:SZ_CARD");
    }

    @Test
    void filtersAllProductsWhoseChannelHasFailedBeforeTryingRemainingCandidates() {
        LegacyRechargeFulfillmentCandidateSelector selector = new LegacyRechargeFulfillmentCandidateSelector(
                (orgCode, parentProductId) -> List.of(
                        departProduct(1L, "SKU-ORIGINAL", 5200L, 20L),
                        departProduct(2L, "SKU-SAME-FAILED-CHANNEL", 5200L, 30L),
                        departProduct(3L, "SKU-STOCK", 5200L, 10L)),
                productIds -> List.of(
                        product("SKU-ORIGINAL", "TF_KA", "原渠道"),
                        product("SKU-SAME-FAILED-CHANNEL", "TF_KA", "同失败渠道"),
                        product("SKU-STOCK", "stock", "库存渠道")));

        var candidates = selector.select("A36", "PARENT-1", "SKU-ORIGINAL", Set.of("TF_KA"));

        assertThat(candidates)
                .extracting(candidate -> candidate.selectedProductId() + ":" + candidate.supplierChannel())
                .containsExactly("SKU-STOCK:stock");
    }

    @Test
    void filtersFailedChannelsCaseInsensitively() {
        LegacyRechargeFulfillmentCandidateSelector selector = new LegacyRechargeFulfillmentCandidateSelector(
                (orgCode, parentProductId) -> List.of(
                        departProduct(1L, "SKU-ORIGINAL", 5200L, 20L),
                        departProduct(2L, "SKU-STOCK", 5200L, 10L)),
                productIds -> List.of(
                        product("SKU-ORIGINAL", "TF_KA", "原渠道"),
                        product("SKU-STOCK", "stock", "库存渠道")));

        var candidates = selector.select("A36", "PARENT-1", "SKU-ORIGINAL", Set.of("tf_ka"));

        assertThat(candidates)
                .extracting(candidate -> candidate.selectedProductId() + ":" + candidate.supplierChannel())
                .containsExactly("SKU-STOCK:stock");
    }

    @Test
    void filtersNonRechargeAndBelowBasePriceProductsFromFailoverCandidates() {
        LegacyRechargeFulfillmentCandidateSelector selector = new LegacyRechargeFulfillmentCandidateSelector(
                (orgCode, parentProductId) -> List.of(
                        departProduct(1L, "SKU-NON-RECHARGE", 5400L, 99L),
                        departProduct(2L, "SKU-BELOW-BASE", 4800L, 98L),
                        departProduct(3L, "SKU-HEALTHY", 5300L, 10L)),
                productIds -> List.of(
                        product("SKU-NON-RECHARGE", "stock", "非直充商品", 0, 4900L),
                        product("SKU-BELOW-BASE", "TF_KA", "低于底价商品", 1, 4900L),
                        product("SKU-HEALTHY", "SZ_CARD", "健康直充商品", 1, 4900L)));

        var candidates = selector.select("A36", "PARENT-1", "SKU-NON-RECHARGE", Set.of());

        assertThat(candidates)
                .extracting(candidate -> candidate.selectedProductId() + ":" + candidate.supplierChannel())
                .containsExactly("SKU-HEALTHY:SZ_CARD");
    }

    private static McDepartProductDO departProduct(Long id, String productId, Long price, Long sort) {
        McDepartProductDO departProduct = new McDepartProductDO();
        departProduct.setId(id);
        departProduct.setOrgCode("A36");
        departProduct.setParentId("PARENT-1");
        departProduct.setProductId(productId);
        departProduct.setPrice(price);
        departProduct.setSort(sort);
        departProduct.setStatus("1");
        return departProduct;
    }

    private static McProductDO product(String productId, String channel, String productName) {
        return product(productId, channel, productName, 1, 4900L);
    }

    private static McProductDO product(
            String productId,
            String channel,
            String productName,
            Integer productType,
            Long basePrice) {
        McProductDO product = new McProductDO();
        product.setProductId(productId);
        product.setChannel(channel);
        product.setProductName(productName);
        product.setType(productType);
        product.setBasePrice(basePrice);
        product.setStatus("1");
        return product;
    }
}

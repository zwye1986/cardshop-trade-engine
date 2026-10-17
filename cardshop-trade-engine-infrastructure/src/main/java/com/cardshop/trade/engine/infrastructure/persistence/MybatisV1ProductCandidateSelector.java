package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.V1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartProductDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McProductDO;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 基于旧卡管商品表的 V1 直充商品候选选择器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * `/v1/cardapi/order/createRechargeOrder` 接口文档中的 {@code product_id} 是平台提供给商户请求的
 * 父产品号；旧卡管按 {@code mc_depart_product.parent_id} 拉取候选子商品，再用子商品
 * {@code product_id} 作为实际派单商品。
 */
public class MybatisV1ProductCandidateSelector implements V1ProductCandidateSelector {

    private static final int DIRECT_RECHARGE_PRODUCT_TYPE = 1;
    private static final long DEFAULT_SORT_VALUE = 0L;
    private static final long DEFAULT_PRODUCT_CONFIG_VERSION = 1L;

    private final V1DepartProductReader departProductReader;
    private final V1ProductReader productReader;

    public MybatisV1ProductCandidateSelector(
            V1DepartProductReader departProductReader,
            V1ProductReader productReader) {
        this.departProductReader = departProductReader;
        this.productReader = productReader;
    }

    @Override
    public V1ProductSelection select(String orgCode, String productId, String account) {
        List<McDepartProductDO> departProducts = departProductReader.selectEnabledByOrgAndProduct(orgCode, productId);
        if (departProducts.isEmpty()) {
            return V1ProductSelection.failed(productId, "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        }

        List<McDepartProductDO> parentDepartProducts = parentDepartProducts(productId, departProducts);
        List<String> productIds = parentDepartProducts.stream()
                .map(McDepartProductDO::getProductId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (productIds.isEmpty()) {
            return V1ProductSelection.failed(productId, "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        }
        /*
         * 参见详细设计「5. Domain 状态与业务规则」：Mapper 层只做单表查询，避免在新服务中复制旧 XML join。
         * 这里先按请求父商品拿到商户授权行，再用授权行里的子商品 product_id 读取商品配置；后续价格、
         * 类型、面值、版本和派单渠道都来自最终选中的同一个子商品快照。
         */
        Map<String, McProductDO> products = productReader.selectEnabledByProductIds(productIds).stream()
                .collect(Collectors.toMap(McProductDO::getProductId, Function.identity(), (left, right) -> left));
        List<Candidate> candidates = parentDepartProducts.stream()
                .map(departProduct -> new Candidate(departProduct, products.get(departProduct.getProductId())))
                .filter(candidate -> candidate.product() != null)
                .sorted(legacyCandidateComparator())
                .toList();
        if (candidates.isEmpty()) {
            return V1ProductSelection.failed(productId, "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        }

        Candidate selected = candidates.getFirst();
        V1ProductSelection snapshot = toSelection(productId, selected, ValidationStatus.PASSED, "", "");
        if (productType(selected.product()) != DIRECT_RECHARGE_PRODUCT_TYPE) {
            return toSelection(productId, selected, ValidationStatus.FAILED, "NOT_RECHARGE_PRODUCT", "该商品不是直充产品！");
        }
        if (price(selected.departProduct()) < basePrice(selected.product())) {
            return toSelection(productId, selected, ValidationStatus.FAILED, "PRICE_INVALID", "卡劵价格异常！");
        }
        return snapshot;
    }

    private static List<McDepartProductDO> parentDepartProducts(String productId, List<McDepartProductDO> departProducts) {
        /*
         * 生产 Mapper 已经使用 parent_id 过滤；这里再做一次防御性过滤，是为了防止测试替身、
         * 未来局部实现或数据访问回退误把其它父商品的授权行返回给 selector。
         */
        return departProducts.stream()
                .filter(departProduct -> Objects.equals(productId, departProduct.getParentId()))
                .toList();
    }

    private static Comparator<Candidate> legacyCandidateComparator() {
        /*
         * 旧卡管 listByProductId 的排序是 stock 渠道优先，其它渠道按 sort desc。这里补上 product_id/id
         * 兜底，避免同 sort 脏数据在多实例间抖动。
         */
        return Comparator
                .comparingInt((Candidate candidate) -> stockPriority(candidate.product()))
                .thenComparing(Comparator.comparingLong((Candidate candidate) -> sort(candidate.departProduct())).reversed())
                .thenComparing(candidate -> defaultString(candidate.product().getProductId()))
                .thenComparingLong(candidate -> id(candidate.departProduct()));
    }

    private static V1ProductSelection toSelection(
            String productId,
            Candidate selected,
            ValidationStatus validationStatus,
            String failCode,
            String failReason) {
        McDepartProductDO selectedDepartProduct = selected.departProduct();
        McProductDO selectedProduct = selected.product();
        /*
         * 请求 productId 保留为旧表 parent_id；授权行 product_id 是实际派单子商品，对应旧表
         * produt_code 和 Redis selected_product_id。参见详细设计「5. Domain 状态与业务规则」。
         */
        return new V1ProductSelection(
                productId,
                selectedDepartProduct.getProductId(),
                defaultString(selectedProduct.getChannel()),
                defaultString(selectedProduct.getProductName()),
                defaultString(selectedProduct.getCouponType()),
                productType(selectedProduct),
                price(selectedDepartProduct),
                value(selectedProduct),
                basePrice(selectedProduct),
                version(selectedProduct),
                true,
                validationStatus,
                failCode,
                failReason).withSupplierBatchId(selectedProduct.getExternalProductId());
    }

    private static long price(McDepartProductDO departProduct) {
        return departProduct.getPrice() == null ? 0L : departProduct.getPrice();
    }

    private static long sort(McDepartProductDO departProduct) {
        return departProduct.getSort() == null ? DEFAULT_SORT_VALUE : departProduct.getSort();
    }

    private static long id(McDepartProductDO departProduct) {
        return departProduct.getId() == null ? Long.MAX_VALUE : departProduct.getId();
    }

    private static int stockPriority(McProductDO product) {
        return "stock".equals(defaultString(product.getChannel())) ? 0 : 99;
    }

    private static long value(McProductDO product) {
        return product.getValue() == null ? 0L : product.getValue();
    }

    private static long basePrice(McProductDO product) {
        return product.getBasePrice() == null ? 0L : product.getBasePrice();
    }

    private static int productType(McProductDO product) {
        return product.getType() == null ? 0 : product.getType();
    }

    private static long version(McProductDO product) {
        return product.getVersion() == null || product.getVersion() <= 0
                ? DEFAULT_PRODUCT_CONFIG_VERSION
                : product.getVersion();
    }

    private static String defaultString(String value) {
        return value == null ? "" : value;
    }

    private record Candidate(McDepartProductDO departProduct, McProductDO product) {
    }
}

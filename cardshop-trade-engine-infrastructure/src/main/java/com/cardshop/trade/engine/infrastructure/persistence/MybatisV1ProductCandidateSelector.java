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
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.5 商品选择规则」。
 * 旧卡管 `listByProductId` 通过多表关联取商户可售子商品；本实现拆成两次单表查询后在内存里组装，
 * 并按确定性规则选择候选，避免多节点因数据库默认顺序不同选到不同渠道。
 */
public class MybatisV1ProductCandidateSelector implements V1ProductCandidateSelector {

    private static final int DIRECT_RECHARGE_PRODUCT_TYPE = 1;
    private static final long DEFAULT_SORT_VALUE = 0L;
    private static final long DEFAULT_PRODUCT_CONFIG_VERSION = 1L;
    private static final int STOCK_CHANNEL_PRIORITY = 0;
    private static final int DEFAULT_CHANNEL_PRIORITY = 99;
    private static final String STOCK_CHANNEL = "stock";

    private final V1DepartProductReader departProductReader;
    private final V1ProductReader productReader;

    public MybatisV1ProductCandidateSelector(
            V1DepartProductReader departProductReader,
            V1ProductReader productReader) {
        this.departProductReader = departProductReader;
        this.productReader = productReader;
    }

    @Override
    public V1ProductSelection select(String orgCode, String parentProductId, String account) {
        List<McDepartProductDO> departProducts = departProductReader.selectEnabledByOrgAndParent(orgCode, parentProductId);
        if (departProducts.isEmpty()) {
            return V1ProductSelection.failed(parentProductId, "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        }

        List<String> productIds = departProducts.stream()
                .map(McDepartProductDO::getProductId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (productIds.isEmpty()) {
            return V1ProductSelection.failed(parentProductId, "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        }
        Map<String, McProductDO> products = productReader.selectEnabledByProductIds(productIds).stream()
                .collect(Collectors.toMap(McProductDO::getProductId, Function.identity(), (left, right) -> left));
        List<Candidate> candidates = departProducts.stream()
                .map(departProduct -> new Candidate(departProduct, products.get(departProduct.getProductId())))
                .filter(candidate -> candidate.product() != null)
                .sorted(candidateComparator())
                .toList();
        if (candidates.isEmpty()) {
            return V1ProductSelection.failed(parentProductId, "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        }

        Candidate selected = candidates.getFirst();
        V1ProductSelection snapshot = toSelection(parentProductId, selected, ValidationStatus.PASSED, "", "");
        if (productType(selected.product()) != DIRECT_RECHARGE_PRODUCT_TYPE) {
            return toSelection(parentProductId, selected, ValidationStatus.FAILED, "NOT_RECHARGE_PRODUCT", "该商品不是直充产品！");
        }
        if (price(selected.departProduct()) < basePrice(selected.product())) {
            return toSelection(parentProductId, selected, ValidationStatus.FAILED, "PRICE_INVALID", "卡劵价格异常！");
        }
        return snapshot;
    }

    private static Comparator<Candidate> candidateComparator() {
        return Comparator
                .comparingLong((Candidate candidate) -> price(candidate.departProduct())).reversed()
                .thenComparing(Comparator.comparingLong((Candidate candidate) -> sort(candidate.departProduct())).reversed())
                .thenComparingInt(candidate -> channelPriority(candidate.product().getChannel()))
                .thenComparing(candidate -> defaultString(candidate.product().getProductId()))
                .thenComparingLong(candidate -> id(candidate.departProduct()));
    }

    private static V1ProductSelection toSelection(
            String parentProductId,
            Candidate candidate,
            ValidationStatus validationStatus,
            String failCode,
            String failReason) {
        McDepartProductDO departProduct = candidate.departProduct();
        McProductDO product = candidate.product();
        return new V1ProductSelection(
                parentProductId,
                departProduct.getProductId(),
                defaultString(product.getChannel()),
                defaultString(product.getProductName()),
                defaultString(product.getCouponType()),
                productType(product),
                price(departProduct),
                value(product),
                basePrice(product),
                version(product),
                validationStatus,
                failCode,
                failReason);
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

    private static int channelPriority(String channel) {
        return STOCK_CHANNEL.equalsIgnoreCase(channel) ? STOCK_CHANNEL_PRIORITY : DEFAULT_CHANNEL_PRIORITY;
    }

    private static String defaultString(String value) {
        return value == null ? "" : value;
    }

    private record Candidate(McDepartProductDO departProduct, McProductDO product) {
    }
}

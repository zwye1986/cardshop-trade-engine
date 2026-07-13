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
 * 旧卡管 `listByProductId` 通过多表关联取商户可售子商品；本实现拆成两次单表查询后在内存里组装，
 * 并按确定性规则选择候选，避免多节点因数据库默认顺序不同选到不同渠道。
 *
 * <p>这里要同时兼容两类历史语义：一是旧卡管按 `stock` 优先、`sort` 倒序选择实际派单子商品；
 * 二是旧卡管用同一父商品下最高商户售价冻结订单金额、面值、券类型和商品配置版本。为避免这两个快照跨到
 * 非直充或低于底价的子商品，新链路先收敛出“健康直充候选”集合，再分别计算派单目标和最高价金额快照。
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
        /*
         * 参见详细设计「5. Domain 状态与业务规则」：Mapper 层只做单表查询，避免在新服务中复制旧 XML join。
         * 这里用 product_id 把 mc_depart_product 的商户售价和 mc_product 的供应商配置重新拼成候选快照。
         */
        Map<String, McProductDO> products = productReader.selectEnabledByProductIds(productIds).stream()
                .collect(Collectors.toMap(McProductDO::getProductId, Function.identity(), (left, right) -> left));
        List<Candidate> candidates = departProducts.stream()
                .map(departProduct -> new Candidate(departProduct, products.get(departProduct.getProductId())))
                .filter(candidate -> candidate.product() != null)
                .sorted(legacyDispatchComparator())
                .toList();
        if (candidates.isEmpty()) {
            return V1ProductSelection.failed(parentProductId, "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        }

        /*
         * 旧卡管 validateProductVo 会先按 listByProductId 的 stock/sort 顺序拿到候选列表，
         * 再用 max(price) 的候选做金额快照；真正写入 order.produt_code/url 的是循环中第一个可发货候选。
         * 新链路按详细设计「5. Domain 状态与业务规则」只处理 product_type=1，且不能把低于底价的
         * 子商品放入派单集合，因此先过滤健康直充候选，再在同一集合里分别取派单候选和最高价快照。
         */
        List<Candidate> healthyCandidates = candidates.stream()
                .filter(MybatisV1ProductCandidateSelector::isDirectRechargeProduct)
                .filter(MybatisV1ProductCandidateSelector::isPriceHealthy)
                .toList();
        if (healthyCandidates.isEmpty()) {
            return invalidSelection(parentProductId, candidates);
        }

        Candidate selected = healthyCandidates.getFirst();
        Candidate pricedSnapshot = highestPriceCandidate(healthyCandidates, selected);
        V1ProductSelection snapshot = toSelection(parentProductId, selected, pricedSnapshot, ValidationStatus.PASSED, "", "");
        return snapshot;
    }

    private static V1ProductSelection invalidSelection(String parentProductId, List<Candidate> candidates) {
        /*
         * 候选整体无健康直充商品时仍要返回一个可落库的失败快照，保持失败订单可追溯。
         * 失败原因优先按最高价候选判断，是为了复刻旧卡管 validateProductVo 对商品类型和价格异常的提示口径；
         * 该结果不会进入供应商派单，因为 validationStatus 会被写成 FAILED。参见详细设计「5. Domain 状态与业务规则」。
         */
        Candidate selected = candidates.getFirst();
        Candidate pricedSnapshot = highestPriceCandidate(candidates, selected);
        if (!isDirectRechargeProduct(pricedSnapshot)) {
            return toSelection(parentProductId, selected, pricedSnapshot,
                    ValidationStatus.FAILED, "NOT_RECHARGE_PRODUCT", "该商品不是直充产品！");
        }
        if (!isPriceHealthy(pricedSnapshot)) {
            return toSelection(parentProductId, selected, pricedSnapshot,
                    ValidationStatus.FAILED, "PRICE_INVALID", "卡劵价格异常！");
        }
        return toSelection(parentProductId, selected, pricedSnapshot,
                ValidationStatus.FAILED, "PRODUCT_NO_PERMISSION", "商品无权限");
    }

    private static Candidate highestPriceCandidate(List<Candidate> candidates, Candidate fallback) {
        /*
         * candidates 已按 legacyDispatchComparator 排过序；max(price) 在价格并列时保留 encounter order 的
         * 第一个元素，因此 productConfigVersion 不依赖数据库默认返回顺序。参见详细设计「5. Domain 状态与业务规则」。
         */
        return candidates.stream()
                .max(Comparator.comparingLong(candidate -> price(candidate.departProduct())))
                .orElse(fallback);
    }

    private static Comparator<Candidate> legacyDispatchComparator() {
        /*
         * 旧卡管 `listByProductId` 的 ORDER BY 是 stock 渠道优先、sort 倒序。
         * productId 和关系表 id 只作为确定性兜底，防止同 sort 时不同数据库实例返回顺序不同，
         * 进而造成同一父商品在灰度多实例间派到不同供应商。参见详细设计「5. Domain 状态与业务规则」。
         */
        return Comparator
                .comparingInt((Candidate candidate) -> channelPriority(candidate.product().getChannel()))
                .thenComparing(Comparator.comparingLong((Candidate candidate) -> sort(candidate.departProduct())).reversed())
                .thenComparing(candidate -> defaultString(candidate.product().getProductId()))
                .thenComparingLong(candidate -> id(candidate.departProduct()));
    }

    private static V1ProductSelection toSelection(
            String parentProductId,
            Candidate selected,
            Candidate pricedSnapshot,
            ValidationStatus validationStatus,
            String failCode,
            String failReason) {
        McDepartProductDO selectedDepartProduct = selected.departProduct();
        McProductDO selectedProduct = selected.product();
        McDepartProductDO pricedDepartProduct = pricedSnapshot.departProduct();
        McProductDO pricedProduct = pricedSnapshot.product();
        /*
         * `selected` 写入旧表 produt_code/supplier_channel/product_name，代表真实供应商履约目标；
         * `pricedSnapshot` 写入 order_price/product_type/coupon_type/value/version，代表旧卡管最高价冻结快照。
         * 二者允许不同，但只允许来自同一个健康直充候选集合，避免金额和派单商品跨到非直充或低价异常配置。
         * 参见详细设计「5. Domain 状态与业务规则」。
         */
        return new V1ProductSelection(
                parentProductId,
                selectedDepartProduct.getProductId(),
                defaultString(selectedProduct.getChannel()),
                defaultString(selectedProduct.getProductName()),
                defaultString(pricedProduct.getCouponType()),
                productType(pricedProduct),
                price(pricedDepartProduct),
                value(pricedProduct),
                basePrice(pricedProduct),
                version(pricedProduct),
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

    private static boolean isDirectRechargeProduct(Candidate candidate) {
        return productType(candidate.product()) == DIRECT_RECHARGE_PRODUCT_TYPE;
    }

    private static boolean isPriceHealthy(Candidate candidate) {
        return price(candidate.departProduct()) >= basePrice(candidate.product());
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

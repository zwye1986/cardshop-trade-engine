package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentCandidate;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartProductDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McProductDO;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 旧卡管派单候选选择器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。生产实现需要复刻旧
 * `listByProductIdDIY` 的顺序：原商品优先、stock 优先、sort 倒序，并排除已经写入失败流水的渠道。
 *
 * <p>注意：旧系统 SQL 本身没有过滤商品类型和底价异常，但新链路设计明确只处理交行直充。
 * 因此本选择器在排序前先应用和受理阶段一致的健康候选规则，防止供应商业务失败后的 failover
 * 把非直充或低于底价的子商品重新放回派单链路。
 */
class LegacyRechargeFulfillmentCandidateSelector {

    private static final int DIRECT_RECHARGE_PRODUCT_TYPE = 1;
    private static final long DEFAULT_SORT_VALUE = 0L;
    private static final int ORIGINAL_PRODUCT_PRIORITY = 0;
    private static final int STOCK_CHANNEL_PRIORITY = 1;
    private static final int DEFAULT_CHANNEL_PRIORITY = 99;
    private static final String STOCK_CHANNEL = "stock";

    private final V1DepartProductReader departProductReader;
    private final V1ProductReader productReader;

    LegacyRechargeFulfillmentCandidateSelector(
            V1DepartProductReader departProductReader,
            V1ProductReader productReader) {
        this.departProductReader = Objects.requireNonNull(departProductReader, "departProductReader must not be null");
        this.productReader = Objects.requireNonNull(productReader, "productReader must not be null");
    }

    List<RechargeFulfillmentCandidate> select(
            String orgCode,
            String parentProductId,
            String originalProductId,
            Set<String> failedChannels) {
        List<McDepartProductDO> departProducts = departProductReader.selectEnabledByOrgAndParent(orgCode, parentProductId);
        if (departProducts.isEmpty()) {
            return List.of();
        }
        List<String> productIds = departProducts.stream()
                .map(McDepartProductDO::getProductId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (productIds.isEmpty()) {
            return List.of();
        }
        Map<String, McProductDO> products = productReader.selectEnabledByProductIds(productIds).stream()
                .collect(Collectors.toMap(McProductDO::getProductId, Function.identity(), (left, right) -> left));
        /*
         * 旧表 mc_order_supplier_flow.status 可能来自历史补数或人工处理，大小写不一定一致。
         * 这里先归一化 channel，后续按渠道整体排除失败供应商，避免同一供应商渠道在一次订单内重复尝试。
         */
        Set<String> failedChannelSet = failedChannels == null
                ? Set.of()
                : failedChannels.stream()
                .map(LegacyRechargeFulfillmentCandidateSelector::normalizeChannel)
                .filter(channel -> !channel.isBlank())
                .collect(Collectors.toSet());
        return departProducts.stream()
                .map(departProduct -> new Candidate(departProduct, products.get(departProduct.getProductId())))
                .filter(candidate -> candidate.product() != null)
                .filter(LegacyRechargeFulfillmentCandidateSelector::isDispatchHealthy)
                .filter(candidate -> !failedChannelSet.contains(normalizeChannel(candidate.product().getChannel())))
                .sorted(legacyDiyComparator(originalProductId))
                .map(candidate -> new RechargeFulfillmentCandidate(
                        defaultString(candidate.departProduct().getProductId()),
                        defaultString(candidate.product().getChannel()),
                        defaultString(candidate.product().getProductName())))
                .toList();
    }

    private static Comparator<Candidate> legacyDiyComparator(String originalProductId) {
        /*
         * 旧卡管 `listByProductIdDIY` 的首键是订单当前 produt_code，因此重试时会先尝试原商品；
         * 原商品失败后再按 stock 优先、sort 倒序寻找可替换供应商。productId/id 是新服务补充的稳定兜底键，
         * 只在旧排序完全并列时生效。参见详细设计「9.1 派单 Outbox」。
         */
        return Comparator
                .comparingInt((Candidate candidate) -> candidatePriority(candidate, originalProductId))
                .thenComparing(Comparator.comparingLong((Candidate candidate) -> sort(candidate.departProduct())).reversed())
                .thenComparing(candidate -> defaultString(candidate.product().getProductId()))
                .thenComparingLong(candidate -> id(candidate.departProduct()));
    }

    private static int candidatePriority(Candidate candidate, String originalProductId) {
        if (Objects.equals(candidate.departProduct().getProductId(), originalProductId)) {
            return ORIGINAL_PRODUCT_PRIORITY;
        }
        if (STOCK_CHANNEL.equalsIgnoreCase(candidate.product().getChannel())) {
            return STOCK_CHANNEL_PRIORITY;
        }
        return DEFAULT_CHANNEL_PRIORITY;
    }

    private static long sort(McDepartProductDO departProduct) {
        return departProduct.getSort() == null ? DEFAULT_SORT_VALUE : departProduct.getSort();
    }

    private static long id(McDepartProductDO departProduct) {
        return departProduct.getId() == null ? Long.MAX_VALUE : departProduct.getId();
    }

    private static boolean isDispatchHealthy(Candidate candidate) {
        /*
         * 参见详细设计「5. Domain 状态与业务规则」：交行新链路只派 product_type=1 的直充商品，
         * 且售价低于底价的子商品不能进入供应商重试集合，避免 failover 绕过受理侧资金校验。
         */
        return productType(candidate.product()) == DIRECT_RECHARGE_PRODUCT_TYPE
                && price(candidate.departProduct()) >= basePrice(candidate.product());
    }

    private static long price(McDepartProductDO departProduct) {
        return departProduct.getPrice() == null ? 0L : departProduct.getPrice();
    }

    private static long basePrice(McProductDO product) {
        return product.getBasePrice() == null ? 0L : product.getBasePrice();
    }

    private static int productType(McProductDO product) {
        return product.getType() == null ? 0 : product.getType();
    }

    private static String normalizeChannel(String channel) {
        return defaultString(channel).toLowerCase(Locale.ROOT);
    }

    private static String defaultString(String value) {
        return value == null ? "" : value;
    }

    private record Candidate(McDepartProductDO departProduct, McProductDO product) {
    }
}

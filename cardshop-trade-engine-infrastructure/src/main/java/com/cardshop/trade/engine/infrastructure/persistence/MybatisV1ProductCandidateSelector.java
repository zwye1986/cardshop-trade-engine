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
 * `/v1/cardapi/order/createRechargeOrder` 接口文档中的 {@code product_id} 是平台提供的产品编号，
 * 新链路按该编号同时匹配 {@code mc_depart_product.product_id} 和 {@code mc_product.product_id}。
 * 旧卡管按 {@code parent_id} 拉取 sibling 商品、再按 stock/sort 或最高价择优的实现存在价格/类型/派单目标
 * 错位风险，本选择器不再复刻那段语义。
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

        List<String> productIds = exactDepartProducts(productId, departProducts).stream()
                .map(McDepartProductDO::getProductId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (productIds.isEmpty()) {
            return V1ProductSelection.failed(productId, "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        }
        /*
         * 参见详细设计「5. Domain 状态与业务规则」：Mapper 层只做单表查询，避免在新服务中复制旧 XML join。
         * 这里用同一个请求 product_id 把商户售价和商品供应商配置重新拼成唯一快照；后续所有价格、类型、
         * 面值、版本和派单渠道都来自这个快照，不能再跨 sibling 商品混用。
         */
        Map<String, McProductDO> products = productReader.selectEnabledByProductIds(productIds).stream()
                .collect(Collectors.toMap(McProductDO::getProductId, Function.identity(), (left, right) -> left));
        List<Candidate> candidates = exactDepartProducts(productId, departProducts).stream()
                .map(departProduct -> new Candidate(departProduct, products.get(departProduct.getProductId())))
                .filter(candidate -> candidate.product() != null)
                .sorted(exactProductDuplicateComparator())
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

    private static List<McDepartProductDO> exactDepartProducts(String productId, List<McDepartProductDO> departProducts) {
        /*
         * 生产 Mapper 已经使用 product_id 精确过滤；这里再做一次防御性过滤，是为了防止测试替身、
         * 未来局部实现或数据访问回退误把同父商品 sibling 返回给 selector。只要不是请求 product_id，
         * 就不能参与受理价格和供应商路由。参见详细设计「5. Domain 状态与业务规则」。
         */
        return departProducts.stream()
                .filter(departProduct -> Objects.equals(productId, departProduct.getProductId()))
                .toList();
    }

    private static Comparator<Candidate> exactProductDuplicateComparator() {
        /*
         * 正常数据下同一商户同一 product_id 只有一条启用授权。若旧库出现重复脏数据，仍要给出稳定结果：
         * 先取售价最高的授权，避免低价重复行导致误判低于底价；再按 sort/id 兜底，避免数据库默认顺序
         * 让 productConfigVersion 或订单金额在多实例间抖动。这里的排序只处理“同一 product_id 的重复授权”，
         * 绝不允许 sibling 商品参与选择。参见详细设计「5. Domain 状态与业务规则」。
         */
        return Comparator
                .comparingLong((Candidate candidate) -> price(candidate.departProduct())).reversed()
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
         * 接口文档 product_id 精确匹配后，Redis product_id、旧表 parent_id、旧表 produt_code 和供应商渠道
         * 都来自同一个商品快照。这样类型校验、售价/底价校验、扣款金额、失败退款金额和实际派单商品保持一致。
         * 参见详细设计「5. Domain 状态与业务规则」。
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

    private static String defaultString(String value) {
        return value == null ? "" : value;
    }

    private record Candidate(McDepartProductDO departProduct, McProductDO product) {
    }
}

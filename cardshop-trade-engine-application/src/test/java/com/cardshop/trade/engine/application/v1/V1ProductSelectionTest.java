package com.cardshop.trade.engine.application.v1;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.domain.order.ValidationStatus;
import org.junit.jupiter.api.Test;

/**
 * V1 完整选品快照指纹契约测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。父快照栅栏必须覆盖
 * 底价、渠道和候选校验结果等全部字段，不能只依赖售价、版本和商品类型。
 */
class V1ProductSelectionTest {

    @Test
    void fingerprintIsStableForSameSnapshot() {
        V1ProductSelection selection = passed(9000L);

        assertThat(selection.cacheFingerprint()).isEqualTo(passed(9000L).cacheFingerprint());
    }

    @Test
    void fingerprintChangesWhenOnlyBasePriceChanges() {
        assertThat(passed(9000L).cacheFingerprint())
                .isNotEqualTo(passed(10001L).cacheFingerprint());
    }

    @Test
    void fingerprintChangesWhenOnlyCandidateValidationChanges() {
        V1ProductSelection passed = passed(9000L);
        V1ProductSelection failed = new V1ProductSelection(
                passed.productId(),
                passed.selectedProductId(),
                passed.supplierChannel(),
                passed.productName(),
                passed.couponType(),
                passed.productType(),
                passed.salePriceCent(),
                passed.faceValueCent(),
                passed.basePriceCent(),
                passed.productConfigVersion(),
                true,
                ValidationStatus.FAILED,
                "PRICE_INVALID",
                "卡劵价格异常！");

        assertThat(passed.cacheFingerprint()).isNotEqualTo(failed.cacheFingerprint());
    }

    private static V1ProductSelection passed(long basePrice) {
        return V1ProductSelection.passed(
                "80100022",
                "80100023",
                "MINISO",
                "名创直充100",
                "codekey",
                1,
                10000L,
                10000L,
                basePrice,
                1L);
    }
}

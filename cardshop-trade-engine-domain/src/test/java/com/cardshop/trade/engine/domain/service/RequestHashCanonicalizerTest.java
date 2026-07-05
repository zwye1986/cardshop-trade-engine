package com.cardshop.trade.engine.domain.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RequestHashCanonicalizerTest {

    private final RequestHashCanonicalizer canonicalizer = new RequestHashCanonicalizer();

    @Test
    void createRechargeHashIgnoresReplayAndSignatureFields() {
        RequestHashCanonicalizer.RechargeCreateFields first = new RequestHashCanonicalizer.RechargeCreateFields(
                "A36",
                "73301280",
                "3f16967313cf43d5986a8828053e5015",
                "18186222336",
                "http://106.14.166.25/hbws/manager/equity/callBackNotice/chengron",
                1
        );

        RequestHashCanonicalizer.RechargeCreateFields second = new RequestHashCanonicalizer.RechargeCreateFields(
                "A36",
                "73301280",
                "3f16967313cf43d5986a8828053e5015",
                "18186222336",
                "http://106.14.166.25/hbws/manager/equity/callBackNotice/chengron",
                1
        );

        RequestHashCanonicalizer.RequestHash firstHash = canonicalizer.canonicalizeRechargeCreate(first);
        RequestHashCanonicalizer.RequestHash secondHash = canonicalizer.canonicalizeRechargeCreate(second);

        assertThat(firstHash.value()).isEqualTo(secondHash.value());
        assertThat(firstHash.canonicalText()).doesNotContain("timestamp", "nonce", "sign", "requestIp");
        assertThat(firstHash.version()).isEqualTo("v1");
        assertThat(firstHash.algorithm()).isEqualTo("SHA-256");
    }

    @Test
    void createRechargeHashChangesWhenBusinessIdentityChanges() {
        RequestHashCanonicalizer.RechargeCreateFields first = new RequestHashCanonicalizer.RechargeCreateFields(
                "A36", "73301280", "OUT-1", "18186222336", "https://notify.example.com/a", 1
        );
        RequestHashCanonicalizer.RechargeCreateFields second = new RequestHashCanonicalizer.RechargeCreateFields(
                "A36", "73301280", "OUT-1", "18186222337", "https://notify.example.com/a", 1
        );

        assertThat(canonicalizer.canonicalizeRechargeCreate(first).value())
                .isNotEqualTo(canonicalizer.canonicalizeRechargeCreate(second).value());
    }

    @Test
    void createRechargeCanonicalTextUsesAsciiSortedFieldNames() {
        RequestHashCanonicalizer.RequestHash hash = canonicalizer.canonicalizeRechargeCreate(
                new RequestHashCanonicalizer.RechargeCreateFields(
                        "A36", "73301280", "OUT-1", "18186222336", "https://notify.example.com/a", 1
                )
        );

        assertThat(hash.canonicalText()).isEqualTo("""
                account=18186222336
                notifyUrl=https://notify.example.com/a
                num=1
                orgCode=A36
                outOrderSn=OUT-1
                productId=73301280
                """);
    }
}

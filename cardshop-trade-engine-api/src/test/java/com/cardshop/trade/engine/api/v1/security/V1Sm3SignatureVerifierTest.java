package com.cardshop.trade.engine.api.v1.security;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderInvalidRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderQueryRequest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class V1Sm3SignatureVerifierTest {

    private final V1Sm3SignatureVerifier verifier = new V1Sm3SignatureVerifier();

    @Test
    void sm3DigestMatchesKnownVector() {
        assertThat(verifier.digestHex("abc"))
                .isEqualTo("66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0");
    }

    @Test
    void createSignatureUsesLegacyAsciiSortedFields() {
        V1RechargeCreateOrderRequest request = new V1RechargeCreateOrderRequest(
                null,
                "OUT-1",
                "73301280",
                null,
                "18186222336",
                1,
                5200L,
                1,
                "http://notify",
                "nonce-32",
                "1783224958",
                "F5DDF1D6176455094137BD110D611AF4C6437CDE5DF2445EE9AC6AA55F0030AF"
        );

        V1SignatureVerifyResult result = verifier.verifyCreate(request, "AES-KEY");

        assertThat(result.source()).isEqualTo(
                "account=18186222336,aesKey=AES-KEY,nonce_str=nonce-32,notify_url=http://notify,num=1,"
                        + "out_order_sn=OUT-1,product_id=73301280,timestamp=1783224958");
        assertThat(result.expectedSign())
                .isEqualTo("f5ddf1d6176455094137bd110d611af4c6437cde5df2445ee9ac6aa55f0030af");
        assertThat(result.valid()).isTrue();
    }

    @Test
    void queryAndInvalidSignatureUseSameLegacyFields() {
        V1RechargeOrderQueryRequest queryRequest = new V1RechargeOrderQueryRequest(
                "OUT-1",
                "nonce-32",
                "1783224958",
                "2e0633bee3e00ed1799c6fa49f6193a5a8fe34b25f47cee9f9dc19db8044e3ee"
        );
        V1RechargeOrderInvalidRequest invalidRequest = new V1RechargeOrderInvalidRequest(
                "OUT-1",
                "reason",
                "nonce-32",
                "1783224958",
                "2e0633bee3e00ed1799c6fa49f6193a5a8fe34b25f47cee9f9dc19db8044e3ee"
        );

        V1SignatureVerifyResult query = verifier.verifyQuery(queryRequest, "AES-KEY");
        V1SignatureVerifyResult invalid = verifier.verifyInvalid(invalidRequest, "AES-KEY");

        assertThat(query.source()).isEqualTo("aesKey=AES-KEY,nonce_str=nonce-32,out_order_sn=OUT-1,timestamp=1783224958");
        assertThat(query.valid()).isTrue();
        assertThat(invalid.source()).isEqualTo(query.source());
        assertThat(invalid.valid()).isTrue();
    }
}

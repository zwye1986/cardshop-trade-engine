package com.cardshop.trade.engine.api.v1.security;
import com.cardshop.trade.engine.api.v1.dto.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static com.cardshop.trade.engine.api.v1.V12TestCredentials.KEY;

class V12SignatureVerifierTest {
    private final V12SignatureVerifier verifier=new V12SignatureVerifier();
    @Test void verifiesWordQueryVectorAndDoesNotExposeAuthenticationMaterial() {
        var request=new V1RechargeOrderQueryRequest("2024051318021328753592","00000000000000000000000000000001","1715592615713","9bee431562361df387186cea6b72e0a4e8d3d56884ae02c09f16d10a4edfc7d3");
        var result=verifier.verifyQuery(request,KEY);
        assertThat(result.valid()).isTrue();
        assertThat(result.source()).isEmpty();
        assertThat(result.expectedSign()).isEmpty();
        assertThat(verifier.verifyInvalid(new V1RechargeOrderInvalidRequest(request.outOrderSn(),"",request.nonceStr(),request.timestamp(),request.sign()),KEY).valid()).isTrue();
    }
    @Test void rejectsLegacySm3AndMissingCredential() {
        var request=new V1RechargeOrderQueryRequest("OUT-1","nonce-32","1783224958","2e0633bee3e00ed1799c6fa49f6193a5a8fe34b25f47cee9f9dc19db8044e3ee");
        assertThat(verifier.verifyQuery(request,KEY).valid()).isFalse();
        assertThat(verifier.verifyQuery(request,null).valid()).isFalse();
    }
}

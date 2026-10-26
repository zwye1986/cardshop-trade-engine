package com.cardshop.trade.engine.infrastructure.notify.bocom;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import com.cardshop.trade.engine.application.recharge.*;
import com.bocom.api.BocomApiException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;

/** 银行投递必须验签并检查双层ACK；参见详细设计2104实施细化。 */
class BocomMerchantNotifyClientAdapterTest {
    private static final ObjectMapper JSON=new ObjectMapper();
    private static final Clock CLOCK=Clock.fixed(Instant.parse("2026-10-21T03:00:00Z"),ZoneId.of("Asia/Shanghai"));
    private static final MerchantNotifyRoute ROUTE=new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI,"A35","r1",
            "https://bank.invalid/api/trade/J_IEP_001_2104/v1","contract","app","RSA","pk1","bk1");
    @Test void bankSenderMustExist() {
        assertThatCode(() -> Class.forName("com.cardshop.trade.engine.infrastructure.notify.bocom.BocomMerchantNotifyClientAdapter"))
                .doesNotThrowAnyException();
    }
    @Test void physicalRetryUsesNewMessageIdAndIdenticalFrozenBusinessBody() throws Exception {
        var p=properties();var audit=new Audit();var ids=new ArrayList<String>();var bodies=new ArrayList<String>();
        var adapter=new BocomMerchantNotifyClientAdapter(p,JSON,audit,CLOCK,(route,pk,bk)->(r,id)-> {
            assertThat(audit.started).contains(id);ids.add(id);
            bodies.add(((Bocom2104Request.Content)r.getBizContent()).getBody().getTrxDataImprt());
            var fields=JSON.readTree(bodies.getLast());
            assertThat(fields.path("operate_type").asText()).isEqualTo("1");
            assertThat(fields.path("total_price").isIntegralNumber()).isTrue();
            return response("S","200");
        });
        var request=request();
        assertThat(adapter.send(request).success()).isTrue();
        p.getMerchants().clear(); // 发送不能重新查当前路由。
        assertThat(adapter.send(request).success()).isTrue();
        assertThat(ids).hasSize(2).doesNotHaveDuplicates();
        assertThat(bodies).containsExactly(request.encodedPayload(),request.encodedPayload());
        assertThat(audit.finished).allSatisfy(result->assertThat(result.status()).isEqualTo("SUCCESS"));
        assertThat(audit.finished.getFirst().refMsgId()).isEqualTo("bank-ref");
    }
    @Test void oldCommaEncodedTaskIsNotRewrittenOrSent() throws Exception {
        var original=request();
        var fields=JSON.readTree(original.payload());
        String legacy="operate_type:1,order_id:O1,out_order_sn:OUT1,total_price:100,type:1,total_settle_price:100,"
                +"timestamp:1792551600000,nonce_str:"+fields.path("nonce_str").asText()+",order_status:12,account:test-account";
        var old=new MerchantNotifyRequest(original.taskId(),original.notifyUrl(),original.payload(),original.orgCode(),
                original.orderId(),original.outOrderSn(),original.deliveryChannel(),original.routeSnapshot(),legacy,
                original.claimVersion(),original.lockedBy(),original.lockedUntil(),original.notifyType(),original.bizStatus());
        var audit=new Audit();
        var sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,audit,CLOCK,
                (route,pk,bk)->(r,id)->response("S","200"));
        assertThat(sender.send(old).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.MANUAL_REQUIRED);
        assertThat(audit.started).isEmpty();
        assertThat(old.encodedPayload()).isEqualTo(legacy);
    }
    @Test void bothGatewayAndBusinessAckAreRequired() throws Exception {
        for(var codes:List.of(List.of("S","500"),List.of("F","200"),List.of("P","200"),List.of("S",""))) {
            var audit=new Audit();
            var sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,audit,CLOCK,
                    (route,pk,bk)->(r,id)->response(codes.get(0),codes.get(1)));
            assertThat(sender.send(request()).success()).isFalse();
            assertThat(audit.finished.getFirst().status()).isEqualTo("RETRYABLE");
        }
    }
    @Test void pausedMissingCredentialAndInitializationFailuresDoNotRegisterPhysicalAttempts() throws Exception {
        var p=properties();var audit=new Audit();
        var sender=new BocomMerchantNotifyClientAdapter(p,JSON,audit,CLOCK,(route,pk,bk)->{throw new IllegalStateException("config secret");});
        p.setEnabled(false);
        assertThat(sender.send(request()).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.DEFERRED);
        p.setEnabled(true);p.getCredentials().clear();
        assertThat(sender.send(request()).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.DEFERRED);
        assertThat(audit.started).isEmpty();
        sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,audit,CLOCK,(route,pk,bk)->{throw new IllegalStateException("config secret");});
        assertThat(sender.send(request()).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.DEFERRED);
        assertThat(audit.started).isEmpty();
    }
    @Test void verifiedSignatureFailureIsManualButNetworkResultIsRetryable() throws Exception {
        var sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,new Audit(),CLOCK,
                (route,pk,bk)->(r,id)->{throw new BocomApiException("Bocom sign verify not passed.");});
        assertThat(sender.send(request()).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.MANUAL_REQUIRED);
        sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,new Audit(),CLOCK,
                (route,pk,bk)->(r,id)->{throw new java.net.SocketTimeoutException("secret");});
        assertThat(sender.send(request()).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.RETRYABLE);
    }
    @Test void lostLeaseOrRegistrationFailureNeverCallsBank() throws Exception {
        var audit=new Audit();audit.canBegin=false;
        var sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,audit,CLOCK,
                (route,pk,bk)->(r,id)->{throw new AssertionError("no physical send");});
        assertThat(sender.send(request()).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.DEFERRED);
        assertThat(audit.finished).isEmpty();
    }
    @Test void corruptedFrozenIdentityCannotBeSignedOrSent() throws Exception {
        var original=request();
        var corrupt=new MerchantNotifyRequest(original.taskId(),original.notifyUrl(),original.payload().replace("O1","OTHER"),
                original.orgCode(),original.orderId(),original.outOrderSn(),original.deliveryChannel(),original.routeSnapshot(),
                original.encodedPayload(),original.claimVersion(),original.lockedBy(),original.lockedUntil());
        var audit=new Audit();
        var sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,audit,CLOCK,
                (route,pk,bk)->{throw new AssertionError("no client preparation");});
        assertThat(sender.send(corrupt).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.MANUAL_REQUIRED);
        assertThat(audit.started).isEmpty();
    }
    @Test void frozenOperationMetadataMustAgreeWithBody() throws Exception {
        var original=request();
        var corrupt=new MerchantNotifyRequest(original.taskId(),original.notifyUrl(),original.payload(),original.orgCode(),
                original.orderId(),original.outOrderSn(),original.deliveryChannel(),original.routeSnapshot(),original.encodedPayload(),
                original.claimVersion(),original.lockedBy(),original.lockedUntil(),"VOUCHER_STATE","CANCELLED");
        var audit=new Audit();
        var sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,audit,CLOCK,
                (route,pk,bk)->(r,id)->response("S","200"));
        assertThat(sender.send(corrupt).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.MANUAL_REQUIRED);
        assertThat(audit.started).isEmpty();
    }
    @Test void malformedSignatureVerifierExceptionIsAlsoManualAndAuditCodesFitDdl() throws Exception {
        var sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,new Audit(),CLOCK,
                (route,pk,bk)->(r,id)->{throw new BocomApiException("sign verify exception.");});
        assertThat(sender.send(request()).disposition()).isEqualTo(MerchantNotifySendResult.Disposition.MANUAL_REQUIRED);
        var audit=new Audit();
        sender=new BocomMerchantNotifyClientAdapter(properties(),JSON,audit,CLOCK,
                (route,pk,bk)->(r,id)->response("INVALID_PLATFORM_VALUE_WITH_PRIVATE_CONTENT","500"));
        sender.send(request());
        assertThat(audit.finished.getFirst().bizState().length()).isLessThanOrEqualTo(8);
    }
    static MerchantNotifyRequest request() throws Exception {
        var signer=new V1MerchantNotifyPayloadSigner(com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort.unavailable());
        var order=new RechargeFulfillmentOrder("O1","OUT1","A35","test-account","P","C","MINISO",100,"",12,"DB_PERSISTED","PASSED",true,true);
        var draft=new MerchantNotifyPreparationService((org,url)->Optional.of(ROUTE),signer,null,CLOCK).orderStatus(order,"12",10).orElseThrow();
        return new MerchantNotifyRequest(draft.taskId(),draft.notifyUrl(),JSON.writeValueAsString(draft.payload()),draft.orgCode(),draft.orderId(),
                draft.outOrderSn(),draft.deliveryChannel(),JSON.writeValueAsString(draft.routeSnapshot()),draft.encodedPayload(),1,"worker",LocalDateTime.now(CLOCK).plusMinutes(1),
                draft.notifyType(),draft.bizStatus());
    }
    static BocomNotifyProperties properties() {
        var p=new BocomNotifyProperties();p.setEnabled(true);p.setTlsTrustCertificatePath("offline.cer");
        for(String ref:List.of("pk1","bk1")) {
            var c=new BocomNotifyProperties.Credential();c.setOrgCode("A35");c.setAppId("app");c.setSignatureAlgorithm("RSA");
            c.setPrivateKey("test-only");c.setBankPublicKey("test-only");p.getCredentials().put(ref,c);
        }
        return p;
    }
    static Bocom2104Response response(String state,String code) {
        var r=new Bocom2104Response();r.setBizState(state);r.setRspCode("ok");r.setRefMsgId("bank-ref");
        var b=new Bocom2104Response.Body();b.setCode(code);r.setBody(b);return r;
    }
    static class Audit implements MerchantNotifyAttemptPort {
        final List<String> started=new ArrayList<>();final List<MerchantNotifyAttemptResult> finished=new ArrayList<>();
        boolean canBegin=true;
        public boolean begin(MerchantNotifyRequest r,String id) {if(canBegin)started.add(id);return canBegin;}
        public void finish(MerchantNotifyRequest r,String id,MerchantNotifyAttemptResult result) {finished.add(result);}
    }
}

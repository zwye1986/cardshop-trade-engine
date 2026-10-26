package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.*;

/** 银行通知准备必须独立于旧签名器；参见详细设计「交行 2104 通知实施细化」。 */
class MerchantNotifyPreparationServiceTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-21T03:00:00Z"), ZoneOffset.UTC);
    private static final MerchantNotifyRoute BANK = new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI,
            "A35", "r1", "https://bank.invalid/api/trade/J_IEP_001_2104/v1", "contract-1", "app-1", "RSA", "key-v1", "bank-v1");
    private static final MerchantNotifyPayloadSigner NO_KEY = new MerchantNotifyPayloadSigner() {
        public Map<String,String> signOrderStatus(RechargeFulfillmentOrder o, String s, Instant t) {
            throw new AssertionError("bank must not use merchant HMAC");
        }
        public Map<String,String> signWriteOff(RechargeFulfillmentOrder o, Instant t) {
            throw new AssertionError("bank must not use merchant HMAC");
        }
    };
    @Test void bankPreparationMustExist() {
        assertThatCode(() -> Class.forName(
                "com.cardshop.trade.engine.application.recharge.MerchantNotifyPreparationService"))
                .doesNotThrowAnyException();
    }
    @Test void emptyOrderUrlStillCreatesBankTaskWithoutBusinessKey() throws Exception {
        var task = service((org,url) -> Optional.of(BANK)).orderStatus(order("", 1950), "12", 10).orElseThrow();
        assertThat(task.deliveryChannel()).isEqualTo(NotifyDeliveryChannel.BOCOM_OPENAPI);
        assertThat(task.routeSnapshot()).isEqualTo(BANK);
        assertThat(task.notifyUrl()).isEqualTo(BANK.gatewayUrl());
        assertThat(task.payload().get("total_price")).isEqualTo(1950L);
        assertThat(task.payload()).doesNotContainKeys("sign", "keyId");
        var encoded = new com.fasterxml.jackson.databind.ObjectMapper().readTree(task.encodedPayload());
        assertThat(encoded.path("operate_type").asText()).isEqualTo("1");
        assertThat(encoded.path("total_price").longValue()).isEqualTo(1950);
        assertThat(task.payload().get("timestamp")).isEqualTo("1792551600000");
    }
    @Test void redeemedAndCancelledKeepDistinctFactsAndOriginalEventTimes() throws Exception {
        var service = service((org,url) -> Optional.of(BANK));
        var useTime = LocalDateTime.parse("2026-10-20T08:09:10");
        var redeemed = service.redeemed(order("",100),"event1",useTime,10).orElseThrow();
        var json = new com.fasterxml.jackson.databind.ObjectMapper();
        var use = json.readTree(redeemed.encodedPayload());
        assertThat(use.path("operate_type").asText()).isEqualTo("2");
        assertThat(use.path("use_time").asText()).isEqualTo("2026-10-20 08:09:10");
        assertThat(use.path("invalid_time").asText()).isEmpty();
        assertThat(use.path("state_type").asText()).isEqualTo("3");
        assertThat(use.path("use_state").asText()).isEqualTo("2");
        var cancelled = service.cancelled(order("",100), Instant.parse("2026-10-20T01:02:03Z"),10).orElseThrow();
        var invalid = json.readTree(cancelled.encodedPayload());
        assertThat(invalid.path("operate_type").asText()).isEqualTo("3");
        assertThat(invalid.path("use_time").asText()).isEmpty();
        assertThat(invalid.path("invalid_time").asText()).isEqualTo("2026-10-20 09:02:03");
        assertThat(invalid.path("state_type").asText()).isEqualTo("3");
        assertThat(invalid.path("use_state").asText()).isEqualTo("3");
        assertThat(cancelled.bizStatus()).isEqualTo("CANCELLED");
        assertThat(redeemed.bizStatus()).isEqualTo("REDEEMED");
    }
    @Test void nonceIsFreshAndTaskIdRemainsStableAcrossPreparationReplays() {
        var service = service((org,url) -> Optional.of(BANK));
        var first = service.orderStatus(order("",100),"12",10).orElseThrow();
        var next = service.orderStatus(order("",100),"12",10).orElseThrow();
        assertThat(first.taskId()).isEqualTo(next.taskId());
        assertThat(first.payload().get("nonce_str").toString()).matches("[0-9a-f]{32}");
        assertThat(first.payload().get("nonce_str")).isNotEqualTo(next.payload().get("nonce_str"));
    }
    @Test void directBlankUrlStillSkipsAndNonblankUsesOriginalSigner() {
        var signer = new MerchantNotifyPayloadSigner() {
            public Map<String,String> signOrderStatus(RechargeFulfillmentOrder o,String s,Instant t) {return Map.of("sign","original");}
            public Map<String,String> signWriteOff(RechargeFulfillmentOrder o,Instant t) {return Map.of("sign","original");}
        };
        var service = new MerchantNotifyPreparationService(MerchantNotifyRoutePort.directOnly(),signer,null,CLOCK);
        assertThat(service.orderStatus(order("",100),"12",10)).isEmpty();
        assertThat(service.orderStatus(order("https://merchant.invalid",100),"12",10).orElseThrow().payload().get("sign"))
                .isEqualTo("original");
    }
    @Test void routeErrorsAndForeignIdentityDoNotSilentlySkipBankNotification() {
        assertThatIllegalStateException().isThrownBy(() -> service((org,url) -> {throw new IllegalStateException("missing route");})
                .orderStatus(order("",100),"12",10));
        var foreign = new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI,"OTHER","r",BANK.gatewayUrl(),"p","a","RSA","k","b");
        assertThatIllegalArgumentException().isThrownBy(() -> service((org,url) -> Optional.of(foreign)).orderStatus(order("",100),"12",10));
        assertThatIllegalArgumentException().isThrownBy(() -> new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI,
                "A35","r", "http://bank.invalid/api/trade/J_IEP_001_2104/v1","p","a","RSA","k","b"));
    }
    @Test void rejectsMissingEventTimeOrInvalidAmountsRatherThanSendingGuessedFacts() {
        var service = service((org,url) -> Optional.of(BANK));
        assertThatIllegalArgumentException().isThrownBy(() -> service.orderStatus(order("",-1),"12",10));
        assertThatIllegalArgumentException().isThrownBy(() -> service.cancelled(order("",100),null,10));
    }
    private static MerchantNotifyPreparationService service(MerchantNotifyRoutePort routes) {
        return new MerchantNotifyPreparationService(routes,NO_KEY,null,CLOCK);
    }
    private static RechargeFulfillmentOrder order(String url, long price) {
        return new RechargeFulfillmentOrder("O1","OUT1","A35","test-account","P","C","MINISO",price,url,12,"DB_PERSISTED","PASSED",true,true);
    }
}

package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.bocom.api.security.keygen.SM2KeyGen;
import com.bocom.api.utils.BocomSignature;
import com.bocom.api.utils.WebUtils;
import com.bocom.api.utils.enums.EncryptType;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.domain.notify.MerchantNotifyOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * 真实官方 SDK→HTTPS loopback→独立银行端验签→银行签名响应→真实 adapter 的离线闭环。
 * 参见详细设计「交行 2104 通知实施细化」；没有真实银行、中间件、Nacos 或生产订单。
 */
class Bocom2104DeliveryIntegrationTest {
    private static final ObjectMapper JSON=new ObjectMapper();
    private static final Clock CLOCK=Clock.fixed(Instant.parse("2026-10-21T03:00:00Z"),ZoneId.of("Asia/Shanghai"));
    private static final String PATH="/api/trade/J_IEP_001_2104/v1";

    @Test void sendsAllOperationsWithRsaAndSm2AndKeepsFrozenPayloadOnPhysicalRetry() throws Exception {
        for(String algorithm:List.of("RSA","SM2")) {
            var merchant=keys(algorithm);var bank=keys(algorithm);
            try(var gateway=new Gateway(algorithm,merchant.publicKey(),bank.privateKey())) {
                for(var operation:MerchantNotifyOperation.values()) {
                    gateway.mode="SUCCESS";var audit=new BocomMerchantNotifyClientAdapterTest.Audit();
                    var sender=sender(gateway,algorithm,merchant,bank,audit,1500);
                    var request=request(gateway.url(),algorithm,operation);
                    assertThat(sender.send(request).success()).as("%s %s string200",algorithm,operation).isTrue();
                    gateway.mode="NUMERIC_SUCCESS";
                    assertThat(sender.send(request).success()).as("%s %s numeric200",algorithm,operation).isTrue();
                    assertThat(audit.started).hasSize(2).doesNotHaveDuplicates();
                    assertThat(audit.finished).allSatisfy(r->{assertThat(r.status()).isEqualTo("SUCCESS");assertThat(r.httpStatus()).isEqualTo(200);});
                    var last=gateway.requests.subList(gateway.requests.size()-2,gateway.requests.size());
                    assertThat(last).allSatisfy(o->{assertThat(o.encoded()).isEqualTo(request.encodedPayload());assertThat(o.protocol()).isEqualTo("offline-contract");});
                    assertThat(last.stream().map(Observed::msgId).toList()).containsExactlyElementsOf(audit.started);
                    if(operation!=MerchantNotifyOperation.ORDER_STATUS) {
                        assertThat(last.getFirst().encoded()).contains("2026-10-21 11:12:13");
                        assertThat(last.getFirst().fields().get(operation==MerchantNotifyOperation.REDEEMED?"invalid_time":"use_time")).isEmpty();
                    }
                }
                assertThat(gateway.requests).hasSize(6);
                assertThat(gateway.failures).isEmpty();
            }
        }
    }

    @Test void requiresVerifiedDoubleAckAndClassifiesTransportUncertaintyAndSignatureFailure() throws Exception {
        for(String algorithm:List.of("RSA","SM2")) {
            var merchant=keys(algorithm);var bank=keys(algorithm);
            try(var gateway=new Gateway(algorithm,merchant.publicKey(),bank.privateKey())) {
                for(String mode:List.of("GATEWAY_FAILURE","PENDING","BUSINESS_FAILURE","EMPTY_BODY","EMPTY_RESPONSE","HTTP_FAILURE","TIMEOUT","TAMPERED","WRONG_KEY","MALFORMED_SIGNATURE","INVALID_BIZ_STATE")) {
                    gateway.mode=mode;var audit=new BocomMerchantNotifyClientAdapterTest.Audit();
                    var configuredBank="WRONG_KEY".equals(mode)?keys(algorithm):bank;
                    var sender=sender(gateway,algorithm,merchant,configuredBank,audit,"TIMEOUT".equals(mode)?120:1500);
                    var result=sender.send(request(gateway.url(),algorithm,MerchantNotifyOperation.ORDER_STATUS));
                    boolean manual=Set.of("TAMPERED","WRONG_KEY","MALFORMED_SIGNATURE").contains(mode);
                    assertThat(result.disposition()).as("%s %s",algorithm,mode).isEqualTo(manual?MerchantNotifySendResult.Disposition.MANUAL_REQUIRED:MerchantNotifySendResult.Disposition.RETRYABLE);
                    assertThat(audit.started).hasSize(1);assertThat(audit.finished).hasSize(1);
                    assertThat(audit.finished.getFirst().status()).isEqualTo(manual?"MANUAL_REQUIRED":"RETRYABLE");
                    if("HTTP_FAILURE".equals(mode)) assertThat(audit.finished.getFirst().httpStatus()).isEqualTo(500);
                    if("INVALID_BIZ_STATE".equals(mode)) assertThat(audit.finished.getFirst().bizState()).isEqualTo("UNKNOWN");
                }
                assertThat(gateway.failures).isEmpty();
            }
        }
    }

    @Test void neverForwardsSignedFormAcrossGatewayRedirect() throws Exception {
        var merchant=keys("RSA");var bank=keys("RSA");
        var target=HttpServer.create(new InetSocketAddress("localhost",0),0);var hits=new AtomicInteger();
        target.createContext("/capture",exchange->{hits.incrementAndGet();exchange.getRequestBody().readAllBytes();byte[] bytes="offline target".getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(200,bytes.length);try(var output=exchange.getResponseBody()){output.write(bytes);} });target.start();
        try(var gateway=new Gateway("RSA",merchant.publicKey(),bank.privateKey())) {
            gateway.redirect="http://localhost:"+target.getAddress().getPort()+"/capture";
            var audit=new BocomMerchantNotifyClientAdapterTest.Audit();
            assertThat(sender(gateway,"RSA",merchant,bank,audit,1500).send(request(gateway.url(),"RSA",MerchantNotifyOperation.ORDER_STATUS)).success()).isFalse();
            assertThat(hits.get()).as("signed form must never be replayed to redirect target").isZero();
        } finally {target.stop(0);}
    }

    private BocomMerchantNotifyClientAdapter sender(Gateway gateway,String algorithm,Keys merchant,Keys bank,BocomMerchantNotifyClientAdapterTest.Audit audit,int readMs) {
        var settings=BocomMerchantNotifyClientAdapterTest.properties();settings.setTlsTrustCertificatePath(BocomTestTlsFixture.trustedCertificate().toString());
        settings.setConnectTimeoutMs(1000);settings.setReadTimeoutMs(readMs);
        for(var credential:settings.getCredentials().values()) {credential.setOrgCode("SYNTHETIC");credential.setAppId("offline-app");credential.setSignatureAlgorithm(algorithm);credential.setPrivateKey(merchant.privateKey());credential.setBankPublicKey(bank.publicKey());}
        return new BocomMerchantNotifyClientAdapter(settings,JSON,audit,CLOCK);
    }
    private MerchantNotifyRequest request(String url,String algorithm,MerchantNotifyOperation operation) throws Exception {
        var fields=new LinkedHashMap<String,Object>();fields.put("order_id","O-SYNTHETIC");fields.put("out_order_sn","OUT-SYNTHETIC");
        fields.put("timestamp","1792551600");fields.put("nonce_str","0123456789abcdef0123456789abcdef");fields.put("account","synthetic-account");
        if(operation==MerchantNotifyOperation.ORDER_STATUS) {fields.put("total_price",12345);fields.put("total_settle_price",12000);fields.put("type","1");fields.put("order_status","12");}
        else {fields.put("use_time",operation==MerchantNotifyOperation.REDEEMED?"2026-10-21 11:12:13":"");fields.put("invalid_time",operation==MerchantNotifyOperation.CANCELLED?"2026-10-21 11:12:13":"");fields.put("state_type","3");fields.put("use_state",operation==MerchantNotifyOperation.REDEEMED?"2":"3");}
        var route=new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI,"SYNTHETIC","offline-route",url,"offline-contract","offline-app",algorithm,"pk1","bk1");
        return new MerchantNotifyRequest("task-"+operation,url,JSON.writeValueAsString(fields),"SYNTHETIC","O-SYNTHETIC","OUT-SYNTHETIC",NotifyDeliveryChannel.BOCOM_OPENAPI,JSON.writeValueAsString(route),new Bocom2104PayloadCodec().encode(operation,fields),1,"offline-worker",LocalDateTime.now(CLOCK).plusMinutes(1),operation==MerchantNotifyOperation.ORDER_STATUS?"ORDER_STATUS":"VOUCHER_STATE",operation==MerchantNotifyOperation.ORDER_STATUS?"12":operation.name());
    }
    private record Keys(String privateKey,String publicKey) { @Override public String toString(){return "Keys[redacted]";} }
    private Keys keys(String algorithm) throws Exception {
        if("SM2".equals(algorithm)){var pair=SM2KeyGen.generateKeyPair();return new Keys(SM2KeyGen.getPriKey(pair),SM2KeyGen.getPubKey(pair));}
        var generator=KeyPairGenerator.getInstance("RSA");generator.initialize(2048);var pair=generator.generateKeyPair();
        return new Keys(Base64.getEncoder().encodeToString(pair.getPrivate().getEncoded()),Base64.getEncoder().encodeToString(pair.getPublic().getEncoded()));
    }
    private record Observed(String msgId,String protocol,String encoded,Map<String,String> fields) { }
    private static final class Gateway implements AutoCloseable {
        final HttpsServer server;final ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor();
        final List<Observed> requests=new CopyOnWriteArrayList<>();final List<Throwable> failures=new CopyOnWriteArrayList<>();
        volatile String mode="SUCCESS",redirect;
        Gateway(String algorithm,String merchantPublicKey,String bankPrivateKey) throws Exception {
            var type="RSA".equals(algorithm)?EncryptType.RSA_AND_AES:EncryptType.SM2_AND_SM4;
            server=BocomTestTlsFixture.server("trusted");server.setExecutor(executor);
            server.createContext(PATH,exchange->{
                String responseMode=mode;
                try(exchange) {
                    assertThat(exchange.getRequestMethod()).isEqualTo("POST");
                    assertThat(exchange.getRequestHeaders().getFirst("Content-Type")).startsWith("application/x-www-form-urlencoded");
                    var form=new LinkedHashMap<String,String>();
                    for(String entry:new String(exchange.getRequestBody().readAllBytes(),StandardCharsets.UTF_8).split("&")) {
                        String[] pair=entry.split("=",2);assertThat(pair).hasSize(2);
                        assertThat(form.put(URLDecoder.decode(pair[0],StandardCharsets.UTF_8),URLDecoder.decode(pair[1],StandardCharsets.UTF_8))).isNull();
                    }
                    String sign=form.remove("sign");assertThat(form).containsEntry("app_id","offline-app").containsEntry("charset","UTF-8");
                    assertThat(BocomSignature.verify(type,WebUtils.buildOrderedSignStr(PATH,form),merchantPublicKey,"UTF-8",sign)).isTrue();
                    var biz=JSON.readTree(form.get("biz_content"));assertThat(biz.path("head").isObject()).isTrue();
                    String encoded=biz.path("body").path("trx_data_imprt").asText();var fields=new LinkedHashMap<String,String>();
                    for(String item:encoded.split(",",-1)){int colon=item.indexOf(':');assertThat(colon).isPositive();fields.put(item.substring(0,colon),item.substring(colon+1));}
                    requests.add(new Observed(form.get("msg_id"),biz.path("body").path("protocol").asText(),encoded,fields));
                    if(redirect!=null){exchange.getResponseHeaders().set("Location",redirect);exchange.sendResponseHeaders(307,-1);return;}
                    String selected=responseMode;if("TIMEOUT".equals(selected))Thread.sleep(1500);
                    if("EMPTY_RESPONSE".equals(selected)){exchange.sendResponseHeaders(200,-1);return;}
                    if("HTTP_FAILURE".equals(selected)){exchange.sendResponseHeaders(500,-1);return;}
                    var response=new LinkedHashMap<String,Object>();response.put("rsp_code","0000");response.put("biz_state","GATEWAY_FAILURE".equals(selected)?"F":"PENDING".equals(selected)?"P":"INVALID_BIZ_STATE".equals(selected)?"unexpected state with spaces and excessive length":"S");
                    response.put("ref_msg_id",form.get("msg_id"));response.put("head",Map.of());response.put("body","EMPTY_BODY".equals(selected)?Map.of():Map.of("code","NUMERIC_SUCCESS".equals(selected)?200:"BUSINESS_FAILURE".equals(selected)?"500":"200","msg","offline"));
                    String content=JSON.writeValueAsString(response);String signature=BocomSignature.sign(type,content,bankPrivateKey,"UTF-8");
                    if("TAMPERED".equals(selected))content=content.replace("offline","tampered");
                    if("MALFORMED_SIGNATURE".equals(selected))signature="invalid-base64-signature";
                    byte[] bytes=("{\"rsp_biz_content\":"+content+",\"sign\":\""+signature+"\"}").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type","application/json;charset=UTF-8");exchange.sendResponseHeaders(200,bytes.length);try(var output=exchange.getResponseBody()){output.write(bytes);}
                } catch(java.io.IOException exception) {if(!"TIMEOUT".equals(responseMode))failures.add(exception);}
                catch(Throwable exception) {failures.add(exception);}
            });server.start();
        }
        String url(){return "https://localhost:"+server.getAddress().getPort()+PATH;}
        @Override public void close(){server.stop(0);executor.close();}
    }
}

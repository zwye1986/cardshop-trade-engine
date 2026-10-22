package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.bocom.api.BocomApiException;
import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.domain.notify.MerchantNotifyOperation;
import com.cardshop.trade.engine.domain.order.VoucherUseState;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2104专用投递：可信冻结路由→登记尝试→官方SDK签验→双层ACK。
 * 参见详细设计「交行 2104 通知实施细化」，无商户业务HMAC及按URL猜测通道逻辑。
 */
public final class BocomMerchantNotifyClientAdapter implements MerchantNotifyClientPort {
    private static final Logger LOG=LoggerFactory.getLogger(BocomMerchantNotifyClientAdapter.class);
    private static final String ORDER_SUCCESS=Integer.toString(com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode.SUCCESS.code());
    private static final String ORDER_FAILED=Integer.toString(com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode.FAILED.code());
    private static final String BANK_ACK="200";
    private final BocomNotifyProperties settings;
    private final ObjectMapper json;
    private final MerchantNotifyAttemptPort attempts;
    private final Clock clock;
    private final Exchange exchange;
    private final LogStormGuard logs=LogStormGuard.defaultGuard();

    @FunctionalInterface
    interface Exchange {
        Delivery prepare(MerchantNotifyRoute route,String privateKey,String bankPublicKey) throws Exception;
    }
    @FunctionalInterface interface Delivery {
        Bocom2104Response execute(Bocom2104Request request,String msgId) throws Exception;
    }
    public BocomMerchantNotifyClientAdapter(BocomNotifyProperties settings,ObjectMapper json,
            MerchantNotifyAttemptPort attempts,Clock clock) {
        this(settings,json,attempts,clock,productionExchange(settings));
    }
    private static Exchange productionExchange(BocomNotifyProperties settings) {
        // 复用不可变路由/版本的客户端及连接池，避免每个通知新建HTTP Selector线程。
        var clients=new java.util.concurrent.ConcurrentHashMap<ClientKey,com.bocom.api.DefaultBocomClient>();
        return (route,privateKey,bankPublicKey)->clients.computeIfAbsent(
                new ClientKey(route,settings.getTlsTrustCertificatePath(),settings.getConnectTimeoutMs(),settings.getReadTimeoutMs()),key ->
                BocomSdkClientFactory.create(key.route().appId(),privateKey,bankPublicKey,key.route().signatureAlgorithm(),
                        key.certificatePath(),key.connectMs(),key.readMs()))::execute;
    }
    private record ClientKey(MerchantNotifyRoute route,String certificatePath,int connectMs,int readMs) { }
    BocomMerchantNotifyClientAdapter(BocomNotifyProperties settings,ObjectMapper json,
            MerchantNotifyAttemptPort attempts,Clock clock,Exchange exchange) {
        this.settings=settings;this.json=json;this.attempts=attempts;this.clock=clock;this.exchange=exchange;
    }

    @Override public MerchantNotifySendResult send(MerchantNotifyRequest request) {
        MerchantNotifyRoute route;
        try { route=validate(request); }
        catch (Exception ex) {
            report("INVALID_FROZEN_TASK",request,null,ex);
            return MerchantNotifySendResult.manual("BANK_INVALID_FROZEN_TASK");
        }
        if (!settings.isEnabled()) return MerchantNotifySendResult.deferred("BANK_SENDING_PAUSED");
        var privateCredential=settings.getCredentials().get(route.privateKeyRef());
        var bankCredential=settings.getCredentials().get(route.bankPublicKeyRef());
        if (!matches(privateCredential,route) || !matches(bankCredential,route)
                || blank(privateCredential.getPrivateKey()) || blank(bankCredential.getBankPublicKey())
                || blank(settings.getTlsTrustCertificatePath()))
            return MerchantNotifySendResult.deferred("BANK_FROZEN_CREDENTIAL_UNAVAILABLE");
        long budget=(long)settings.getConnectTimeoutMs()+settings.getReadTimeoutMs();
        var now=LocalDateTime.ofInstant(clock.instant(),ZoneId.of("Asia/Shanghai"));
        if (settings.getConnectTimeoutMs()<=0 || settings.getReadTimeoutMs()<=0
                || request.lockedUntil()==null || !now.plusNanos(Duration.ofMillis(budget).toNanos()).isBefore(request.lockedUntil()))
            return MerchantNotifySendResult.deferred("BANK_LEASE_BUDGET_UNAVAILABLE");
        Delivery delivery;
        try { delivery=exchange.prepare(route,privateCredential.getPrivateKey(),bankCredential.getBankPublicKey()); }
        catch (Exception ex) {
            report("SDK_CONFIGURATION_FAILED",request,null,ex);
            return MerchantNotifySendResult.deferred("BANK_SDK_CONFIGURATION_UNAVAILABLE");
        }
        // 初始化可能耗时，登记前重新检查剩余预算；配置等待不伪造物理请求记录。
        if (!LocalDateTime.ofInstant(clock.instant(),ZoneId.of("Asia/Shanghai"))
                .plusNanos(Duration.ofMillis(budget).toNanos()).isBefore(request.lockedUntil()))
            return MerchantNotifySendResult.deferred("BANK_LEASE_BUDGET_UNAVAILABLE");

        var body=new Bocom2104Request.Body();
        body.setProtocol(route.protocol());body.setTrxDataImprt(request.encodedPayload());
        var content=new Bocom2104Request.Content();content.setHead(new Bocom2104Request.Head());content.setBody(body);
        var sdkRequest=new Bocom2104Request();sdkRequest.setBizContent(content);sdkRequest.setServiceUrl(route.gatewayUrl());
        String msgId=UUID.randomUUID().toString().replace("-","");
        try {
            if (!attempts.begin(request,msgId)) return MerchantNotifySendResult.deferred("BANK_CLAIM_LOST");
        } catch (RuntimeException ex) {
            report("ATTEMPT_REGISTRATION_FAILED",request,msgId,ex);
            return MerchantNotifySendResult.deferred("BANK_ATTEMPT_REGISTRATION_FAILED");
        }

        MerchantNotifySendResult result;
        MerchantNotifyAttemptResult evidence;
        try {
            // SDK execute返回前已验签，不能捕获验签异常后再自行解析原文判成功。
            var response=delivery.execute(sdkRequest,msgId);
            String business=response==null || response.getBody()==null ? null : response.getBody().getCode();
            boolean ack=response!=null && response.isSuccess() && BANK_ACK.equals(business);
            int http=response==null?0:response.getTransportHttpStatus();
            result=ack ? MerchantNotifySendResult.success(http,"")
                    : MerchantNotifySendResult.failed(http,"BANK_NOT_ACKNOWLEDGED");
            // 仅保存受控传输实际观测的状态；没有HTTP响应时为0。
            evidence=new MerchantNotifyAttemptResult(ack?"SUCCESS":"RETRYABLE",http,
                    safe(response==null?null:response.getBizState(),8),safe(response==null?null:response.getRspCode(),64),
                    safe(business,32),safe(response==null?null:response.getRefMsgId(),64));
        } catch (Exception ex) {
            boolean verification=verificationFailure(ex);
            report(verification?"RESPONSE_VERIFY_FAILED":"TRANSPORT_FAILED",request,msgId,ex);
            result=verification?MerchantNotifySendResult.manual("BANK_RESPONSE_VERIFY_FAILED")
                    :MerchantNotifySendResult.failed(0,"BANK_TRANSPORT_UNCERTAIN");
            evidence=new MerchantNotifyAttemptResult(verification?"MANUAL_REQUIRED":"RETRYABLE",
                    ex instanceof BocomTransportException http?http.httpStatus():0,null,null,null,null);
        }
        try { attempts.finish(request,msgId,evidence); }
        catch (RuntimeException ex) {
            report("ATTEMPT_COMPLETION_FAILED",request,msgId,ex);
            // 请求可能已被银行处理；保留未完成尝试并转人工，禁止掩盖审计失败或立即重发。
            return MerchantNotifySendResult.manual("BANK_ATTEMPT_COMPLETION_UNKNOWN");
        }
        return result;
    }

    private MerchantNotifyRoute validate(MerchantNotifyRequest request) throws Exception {
        if (request.deliveryChannel()!=NotifyDeliveryChannel.BOCOM_OPENAPI || blank(request.orgCode())
                || blank(request.orderId()) || blank(request.outOrderSn())) throw new IllegalArgumentException("bank task identity missing");
        var route=json.readValue(request.routeSnapshot(),MerchantNotifyRoute.class);
        if (route.channel()!=NotifyDeliveryChannel.BOCOM_OPENAPI || !request.orgCode().equals(route.orgCode())
                || !route.gatewayUrl().equals(request.notifyUrl())) throw new IllegalArgumentException("bank frozen route mismatch");
        Map<String,Object> payload=json.readValue(request.payload(),new TypeReference<>() {});
        if (!request.orderId().equals(payload.get("order_id")) || !request.outOrderSn().equals(payload.get("out_order_sn")))
            throw new IllegalArgumentException("bank payload ownership mismatch");
        MerchantNotifyOperation operation;
        if ("ORDER_STATUS".equals(request.notifyType())
                && (ORDER_SUCCESS.equals(request.bizStatus()) || ORDER_FAILED.equals(request.bizStatus()))
                && request.bizStatus().equals(payload.get("order_status"))) {
            operation=MerchantNotifyOperation.ORDER_STATUS;
        } else if ("VOUCHER_STATE".equals(request.notifyType()) && "REDEEMED".equals(request.bizStatus())
                && VoucherUseState.REDEEMED.code().equals(payload.get("use_state")) && VoucherUseState.MINISO_STATE_TYPE.equals(payload.get("state_type"))
                && "".equals(payload.get("invalid_time"))) {
            operation=MerchantNotifyOperation.REDEEMED;
            validateFactTime(payload.get("use_time"));
        } else if ("VOUCHER_STATE".equals(request.notifyType()) && "CANCELLED".equals(request.bizStatus())
                && VoucherUseState.CANCELLED.code().equals(payload.get("use_state"))
                && VoucherUseState.isSupportedStateType(String.valueOf(payload.get("state_type")))
                && "".equals(payload.get("use_time"))) {
            operation=MerchantNotifyOperation.CANCELLED;
            validateFactTime(payload.get("invalid_time"));
        } else throw new IllegalArgumentException("bank operation and frozen facts disagree");
        if (!(payload.get("nonce_str") instanceof String nonce) || !nonce.matches("[0-9a-f]{32}")
                || !(payload.get("timestamp") instanceof String timestamp) || !timestamp.matches("[0-9]{1,19}"))
            throw new IllegalArgumentException("bank notification timestamp or nonce invalid");
        String expected=new Bocom2104PayloadCodec().encode(operation,payload);
        if (!expected.equals(request.encodedPayload())) throw new IllegalArgumentException("bank encoded snapshot mismatch");
        return route;
    }
    private static void validateFactTime(Object value) {
        if (!(value instanceof String time)) throw new IllegalArgumentException("bank event time missing");
        LocalDateTime.parse(time,java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
                .withResolverStyle(java.time.format.ResolverStyle.STRICT));
    }
    private static boolean matches(BocomNotifyProperties.Credential credential,MerchantNotifyRoute route) {
        return credential!=null && route.orgCode().equals(credential.getOrgCode())
                && route.appId().equals(credential.getAppId()) && route.signatureAlgorithm().equals(credential.getSignatureAlgorithm());
    }
    private static boolean blank(String value) { return value==null || value.isBlank(); }
    private static String safe(String value,int max) {
        if (value==null) return null;
        return value.matches("[A-Za-z0-9_.:-]{1,"+max+"}")?value:"UNKNOWN";
    }
    private static boolean verificationFailure(Throwable ex) {
        if (ex instanceof BocomTransportException http) return http.verificationFailure();
        return ex instanceof BocomApiException && ex.getMessage()!=null && ex.getMessage().contains("sign verify");
    }
    private void report(String category,MerchantNotifyRequest request,String msgId,Exception ex) {
        var sample=logs.shouldLog("bocom.notify."+category);
        if (sample.shouldLog()) {
            var safe=new IllegalStateException("bank notification failed: "+ex.getClass().getSimpleName());
            safe.setStackTrace(ex.getStackTrace());
            LOG.error("银行通知异常 taskId={},orgCode={},orderId={},msgId={},category={},occurrence={}",
                    request.taskId(),request.orgCode(),request.orderId(),msgId,category,sample.occurrence(),safe);
        }
    }
}

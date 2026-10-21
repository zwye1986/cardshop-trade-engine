package com.cardshop.trade.engine.simulator.bocom;

import com.bocom.api.security.keygen.SM2KeyGen;
import com.bocom.api.utils.BocomSignature;
import com.bocom.api.utils.WebUtils;
import com.bocom.api.utils.enums.EncryptType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 仅监听随机 loopback 端口的离线 2104 银行桩；不包含真实银行地址、协议或密钥。
 * 参见 docs/交行直充业务系统详细设计.md「交行 2104 通知实施细化」。
 * 银行端以独立配置的商户公钥验签；业务事件按身份及操作去重，物理请求仍逐条留作关联证据。
 */
public final class Bocom2104GatewayServer implements AutoCloseable {
    public static final String API_PATH="/api/trade/J_IEP_001_2104/v1";
    private static final Logger LOG=LoggerFactory.getLogger(Bocom2104GatewayServer.class);
    private static final ObjectMapper JSON=new ObjectMapper();
    private static final int MAX_FORM_BYTES=65536;
    public enum Mode { SUCCESS, NUMERIC_SUCCESS, GATEWAY_FAILURE, PENDING, BUSINESS_FAILURE, EMPTY_BODY, EMPTY_RESPONSE, HTTP_FAILURE, TAMPERED, TIMEOUT }
    private final HttpServer server;
    private final ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor();
    private final String appId,merchantPublicKey,bankPrivateKey;
    private final EncryptType type;
    private final List<Observation> observations=new CopyOnWriteArrayList<>();
    private final Set<List<String>> events=ConcurrentHashMap.newKeySet();
    private final List<String> failures=new CopyOnWriteArrayList<>();
    private volatile Mode mode=Mode.SUCCESS;

    public Bocom2104GatewayServer(String appId,String algorithm,String merchantPublicKey,String bankPrivateKey) throws IOException {
        this.appId=Objects.requireNonNull(appId);this.merchantPublicKey=Objects.requireNonNull(merchantPublicKey);
        this.bankPrivateKey=Objects.requireNonNull(bankPrivateKey);this.type=type(algorithm);
        server=HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(),0),0);
        server.createContext(API_PATH,this::handle);server.setExecutor(executor);
    }
    public void start() {server.start();}
    public String url() {return "http://localhost:"+server.getAddress().getPort()+API_PATH;}
    public void mode(Mode mode) {this.mode=Objects.requireNonNull(mode);}
    public List<Observation> observations() {return List.copyOf(observations);}
    public Optional<Observation> find(String msgId) {return observations.stream().filter(o->o.msgId().equals(msgId)).findFirst();}
    public int businessEventCount() {return events.size();}
    public List<String> failures() {return List.copyOf(failures);}
    public record Observation(String msgId,String protocol,String encodedPayload,Map<String,String> fields) { }

    private void handle(HttpExchange exchange) throws IOException {
        try(exchange) {
            try {
                if(!"POST".equals(exchange.getRequestMethod()) || !API_PATH.equals(exchange.getRequestURI().getPath())
                        || !Optional.ofNullable(exchange.getRequestHeaders().getFirst("Content-Type")).orElse("").startsWith("application/x-www-form-urlencoded"))
                    throw new IllegalArgumentException("invalid gateway method or form content type");
                byte[] input=exchange.getRequestBody().readNBytes(MAX_FORM_BYTES+1);
                if(input.length>MAX_FORM_BYTES) throw new IllegalArgumentException("gateway form too large");
                var form=parseForm(new String(input,StandardCharsets.UTF_8));
                String signature=form.remove("sign");
                if(!appId.equals(form.get("app_id")) || !"UTF-8".equalsIgnoreCase(form.get("charset"))
                        || signature==null || !BocomSignature.verify(type,WebUtils.buildOrderedSignStr(API_PATH,form),merchantPublicKey,"UTF-8",signature))
                    throw new IllegalArgumentException("gateway signature or merchant invalid");
                String msgId=required(form.get("msg_id"));
                var biz=JSON.readTree(required(form.get("biz_content")));
                if(!biz.path("head").isObject() || !biz.path("body").isObject()) throw new IllegalArgumentException("gateway head/body missing");
                String protocol=required(biz.path("body").path("protocol").asText(null));
                String text=required(biz.path("body").path("trx_data_imprt").asText(null));
                var fields=parseFields(text);
                String operation=required(fields.get("operate_type"));
                if(!Set.of("1","2","3").contains(operation)) throw new IllegalArgumentException("unknown operation");
                String order=required(fields.get("order_id")),outOrder=required(fields.get("out_order_sn"));
                observations.add(new Observation(msgId,protocol,text,fields));
                events.add(List.of(appId,protocol,operation,order,outOrder,fields.getOrDefault("order_status","")));
                Mode selected=mode;
                if(selected==Mode.TIMEOUT) Thread.sleep(600);
                if(selected==Mode.EMPTY_RESPONSE) {respond(exchange,200,"");return;}
                if(selected==Mode.HTTP_FAILURE) {respond(exchange,500,"offline HTTP failure");return;}
                var response=new LinkedHashMap<String,Object>();
                response.put("rsp_code","0000");response.put("biz_state",selected==Mode.GATEWAY_FAILURE?"F":selected==Mode.PENDING?"P":"S");
                response.put("ref_msg_id",msgId);response.put("head",Map.of());
                response.put("body",selected==Mode.EMPTY_BODY?Map.of():Map.of("code",selected==Mode.NUMERIC_SUCCESS?200:selected==Mode.BUSINESS_FAILURE?"500":"200","msg","offline"));
                String content=JSON.writeValueAsString(response);
                String sign=BocomSignature.sign(type,content,bankPrivateKey,"UTF-8");
                if(selected==Mode.TAMPERED) content=content.replace("offline","tampered");
                respond(exchange,200,"{\"rsp_biz_content\":"+content+",\"sign\":\""+sign+"\"}");
            } catch(IllegalArgumentException exception) {
                // 非可信输入属于预期拒绝，不记录正文、签名或密钥，也避免异常请求制造 error 风暴。
                LOG.warn("离线银行桩拒绝无效请求 category={}",exception.getClass().getSimpleName());
                respond(exchange,400,"invalid offline request");
            } catch(InterruptedException exception) {
                Thread.currentThread().interrupt();throw new IOException("offline server interrupted",exception);
            } catch(Exception exception) {
                failures.add(exception.getClass().getSimpleName());
                // 离线可控低频路径：只保留脱敏异常类型及堆栈，SDK异常消息可能含正文而不能直接输出。
                var safe=new IllegalStateException("offline bank failure: "+exception.getClass().getSimpleName());
                safe.setStackTrace(exception.getStackTrace());LOG.error("离线银行桩处理异常",safe);
                respond(exchange,500,"offline bank failure");
            }
        }
    }
    private static void respond(HttpExchange exchange,int status,String body) throws IOException {
        byte[] bytes=body.getBytes(StandardCharsets.UTF_8);exchange.getResponseHeaders().set("Content-Type","application/json;charset=UTF-8");
        exchange.sendResponseHeaders(status,bytes.length);try(var output=exchange.getResponseBody()) {output.write(bytes);}
    }
    private static Map<String,String> parseForm(String text) {
        var result=new LinkedHashMap<String,String>();
        for(String part:text.split("&",-1)) {
            String[] pair=part.split("=",2);
            if(pair.length!=2 || result.putIfAbsent(URLDecoder.decode(pair[0],StandardCharsets.UTF_8),URLDecoder.decode(pair[1],StandardCharsets.UTF_8))!=null)
                throw new IllegalArgumentException("invalid or duplicate form field");
        }
        return result;
    }
    /** 只在第一个冒号处分割，时间中的冒号与字段空串必须原样保留。 */
    public static Map<String,String> parseFields(String text) {
        if(text.codePointCount(0,text.length())>2500) throw new IllegalArgumentException("business text too large");
        var result=new LinkedHashMap<String,String>();
        for(String part:text.split(",",-1)) {
            int colon=part.indexOf(':');
            if(colon<=0 || result.putIfAbsent(part.substring(0,colon),part.substring(colon+1))!=null)
                throw new IllegalArgumentException("invalid or duplicate business field");
        }
        return Collections.unmodifiableMap(result);
    }
    private static String required(String value) {if(value==null || value.isBlank()) throw new IllegalArgumentException("required gateway field missing");return value;}
    private static EncryptType type(String algorithm) {return switch(algorithm) {case "RSA"->EncryptType.RSA_AND_AES;case "SM2"->EncryptType.SM2_AND_SM4;default->throw new IllegalArgumentException("explicit RSA/SM2 required");};}
    /** 运行时生成合成测试密钥；toString 永不暴露私钥。 */
    public record TestKeys(String privateKey,String publicKey) { @Override public String toString() {return "TestKeys[redacted]";} }
    public static TestKeys generateKeys(String algorithm) throws Exception {
        if(type(algorithm)==EncryptType.SM2_AND_SM4) {var keys=SM2KeyGen.generateKeyPair();return new TestKeys(SM2KeyGen.getPriKey(keys),SM2KeyGen.getPubKey(keys));}
        var generator=KeyPairGenerator.getInstance("RSA");generator.initialize(2048);var keys=generator.generateKeyPair();
        return new TestKeys(Base64.getEncoder().encodeToString(keys.getPrivate().getEncoded()),Base64.getEncoder().encodeToString(keys.getPublic().getEncoded()));
    }
    @Override public void close() {server.stop(0);executor.close();}

    /** 手工启动只生成新银行测试密钥，商户公钥可从第二个参数文件载入；所有私钥均不打印。 */
    public static void main(String[] args) throws Exception {
        if(args.length>2) throw new IllegalArgumentException("usage: [RSA|SM2] [merchant-public-key-file]");
        String algorithm=args.length==0?"RSA":args[0];var bank=generateKeys(algorithm);
        String merchantPublicKey=args.length==2?java.nio.file.Files.readString(java.nio.file.Path.of(args[1]),StandardCharsets.UTF_8).strip():generateKeys(algorithm).publicKey();
        var server=new Bocom2104GatewayServer("offline-app",algorithm,merchantPublicKey,bank.privateKey());server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        System.out.println("Offline 2104 gateway: "+server.url());
        System.out.println("Offline bank response public key: "+bank.publicKey());
        if(args.length<2) System.out.println("No merchant public key supplied: use the Java constructor or supply a synthetic public-key file for SDK requests.");
    }
}

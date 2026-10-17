package com.cardshop.trade.engine.infrastructure.supplier.miniso;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * 名创四个主动 POST 接口的专属协议客户端，不实现重试、业务终态映射或本地库存行为。
 *
 * <p>见 {@code docs/交行直充业务系统详细设计.md}「名创 MINISO 专属协议实施约束（2026-10-17）」。
 * 正文只序列化一次，签名与发送使用同一份 UTF-8 字节；网络异常及中断直接透传供上层判定结果未知。
 * 本层禁止记录密钥、鉴权头、签名原文、手机号与券码，业务上下文采样日志由 Gateway 负责。
 */
public final class MinisoHttpClient {

    private static final String PATH_PREFIX = "/tripartite/coupon/";
    private static final String CONTENT_TYPE = "application/json; charset=UTF-8";
    private static final String NONCE_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz";
    private static final int NONCE_LENGTH = 32;
    private static final int MAX_COUPON_CODES = 10;
    private static final String NONCE_PATTERN = "[a-z0-9]{32}";

    private final MinisoSettings settings;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Supplier<String> nonceSupplier;

    /** 生产构造器使用系统毫秒时钟和安全随机 nonce。 */
    public MinisoHttpClient(MinisoSettings settings, HttpClient httpClient, ObjectMapper objectMapper) {
        this(settings, httpClient, objectMapper, Clock.systemUTC(), secureNonceSupplier());
    }

    /** 时钟与 nonce 可注入以独立校验旧协议签名，生产调用仍需满足 nonce 格式约束。 */
    public MinisoHttpClient(MinisoSettings settings, HttpClient httpClient, ObjectMapper objectMapper,
                            Clock clock, Supplier<String> nonceSupplier) {
        this.settings = Objects.requireNonNull(settings, "MINISO settings are required");
        this.httpClient = Objects.requireNonNull(httpClient, "MINISO HTTP client is required");
        this.objectMapper = Objects.requireNonNull(objectMapper, "MINISO object mapper is required");
        this.clock = Objects.requireNonNull(clock, "MINISO clock is required");
        this.nonceSupplier = Objects.requireNonNull(nonceSupplier, "MINISO nonce supplier is required");
    }

    /** 使用平台原订单号和冻结批次发券，batchId/number 保持 JSON 数值，不生成新的远端幂等号。 */
    public MinisoApiResponse batchGrandCoupon(String phone, int batchId, int number, String orderNo)
            throws IOException, InterruptedException {
        requireNonblank(phone, "MINISO phone is required");
        requireNonblank(orderNo, "MINISO orderNo is required");
        if (batchId <= 0 || number <= 0) {
            throw new IllegalArgumentException("MINISO batchId and number must be positive");
        }
        Map<String, Object> batch = new LinkedHashMap<>();
        batch.put("batchId", batchId);
        batch.put("number", number);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("phone", phone);
        body.put("batchList", List.of(batch));
        body.put("orderNo", orderNo);
        return post("batchGrandCoupon", body);
    }

    /** 查单始终复用平台原订单号；未知结果由 Gateway 和既有恢复链路处理。 */
    public MinisoApiResponse queryGrand(String orderNo) throws IOException, InterruptedException {
        requireNonblank(orderNo, "MINISO orderNo is required");
        return post("queryGrand", Map.of("orderNo", orderNo));
    }

    /** 精确作废本订单已确认的券码，禁止自动拆批以免隐藏部分成功语义。 */
    public MinisoApiResponse abandonUserCoupon(List<String> couponCodes) throws IOException, InterruptedException {
        return post("abandonUserCoupon", Map.of("couponCodes", validatedCouponCodes(couponCodes)));
    }

    /** 按指定券码查询使用状态，只提供协议能力，不启动后台轮询。 */
    public MinisoApiResponse selectUserCouponList(List<String> couponCodes) throws IOException, InterruptedException {
        return post("selectUserCouponList", Map.of("couponCodes", validatedCouponCodes(couponCodes)));
    }

    private MinisoApiResponse post(String operation, Map<String, Object> body)
            throws IOException, InterruptedException {
        String nonce = nonceSupplier.get();
        if (nonce == null || !nonce.matches(NONCE_PATTERN)) {
            throw new IllegalArgumentException("MINISO nonce must contain 32 lowercase alphanumeric characters");
        }
        byte[] bodyBytes;
        try {
            bodyBytes = objectMapper.writeValueAsBytes(body);
        } catch (JsonProcessingException exception) {
            // 立即安全重抛：Jackson 的异常上下文可能包含手机号/券码，禁止附带原始异常进入上层日志。
            throw new IOException("MINISO request JSON serialization failed");
        }
        String time = Long.toString(clock.millis());
        HttpRequest request = HttpRequest.newBuilder(settings.baseUri().resolve(PATH_PREFIX + operation))
                .timeout(settings.timeout())
                .header("Content-Type", CONTENT_TYPE)
                .header("tenant", settings.tenant())
                .header("tripartite", settings.tripartite())
                .header("version", settings.version())
                .header("nonce", nonce)
                .header("time", time)
                .header("sign", signature(nonce, time, bodyBytes))
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .build();
        HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        JsonNode parsed;
        try {
            // 严格读取整份 JSON，不能仅接受有效前缀后忽略尾部损坏或第二份响应内容。
            parsed = objectMapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readTree(response.body());
        } catch (JsonProcessingException exception) {
            // 不携带包含供应商敏感正文的解析异常；安全重抛仍保存原 HTTP 信封供 Gateway 受控审计。
            throw new InvalidResponseException(new MinisoApiResponse(response.statusCode(), null, response.body()));
        }
        if (parsed == null || parsed.isMissingNode()) {
            throw new InvalidResponseException(new MinisoApiResponse(response.statusCode(), null, response.body()));
        }
        return new MinisoApiResponse(response.statusCode(), parsed, response.body());
    }

    private String signature(String nonce, String time, byte[] bodyBytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            // 旧合同只拼接以下命名头；tripartite 仅随请求头传送，不参与签名。
            String prefix = settings.key() + "nonce" + nonce + "tenant" + settings.tenant()
                    + "time" + time + "version" + settings.version();
            digest.update(prefix.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().withUpperCase().formatHex(digest.digest(bodyBytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("Required MINISO MD5 algorithm is unavailable", exception);
        }
    }

    private static String validatedCouponCodes(List<String> couponCodes) {
        if (couponCodes == null || couponCodes.isEmpty() || couponCodes.size() > MAX_COUPON_CODES) {
            throw new IllegalArgumentException("MINISO couponCodes must contain between 1 and 10 items");
        }
        var unique = new HashSet<String>();
        for (String code : couponCodes) {
            // 每个元素就是一张券，禁止内嵌逗号产生空项或绕过数量限制，不擅自修剪供应商券码。
            if (code == null || code.isBlank() || code.indexOf(',') >= 0 || !unique.add(code)) {
                throw new IllegalArgumentException("MINISO couponCodes must contain unique nonblank individual codes");
            }
        }
        return String.join(",", couponCodes);
    }

    private static void requireNonblank(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
    }

    private static Supplier<String> secureNonceSupplier() {
        SecureRandom random = new SecureRandom();
        return () -> {
            char[] nonce = new char[NONCE_LENGTH];
            for (int i = 0; i < nonce.length; i++) {
                nonce[i] = NONCE_ALPHABET.charAt(random.nextInt(NONCE_ALPHABET.length()));
            }
            return new String(nonce);
        };
    }

    /**
     * HTTP 已返回但 JSON 不可解析时保存受控审计信封，不把供应商原文或解析器 cause 放进异常字符串。
     *
     * <p>参见详细设计「名创 MINISO 专属协议实施约束（2026-10-17）」：结果未知仍需留存原始响应，
     * 由 Gateway 提取原文写受控流水；日志只能使用本异常的安全消息及调用栈。
     */
    public static final class InvalidResponseException extends IOException {
        private static final long serialVersionUID = 1L;
        private final MinisoApiResponse response;

        private InvalidResponseException(MinisoApiResponse response) {
            super("MINISO response is not valid JSON");
            this.response = response;
        }

        public MinisoApiResponse response() {
            return response;
        }
    }
}

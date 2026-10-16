package example.v12.crypto;

import org.bouncycastle.crypto.digests.SM3Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.util.encoders.Hex;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * V1.2 报文认证参考工具，无 Spring/JSON 框架依赖，可并发使用。
 * <p>对外协议依据：《交行直充接口技术文档-V1.2-修订稿》1.2、2.2—2.6。
 * 示例包名example.v12.crypto可改为调用方项目自己的包名，无需依赖平台自有组件。
 * aesKey 不进入本类；密钥必须是平台签发的独立32字节 signKey 的规范 Base64URL 编码。
 * <p>本类只负责规范化及密码运算，不查询密钥状态，不处理 token、防重放、幂等和业务必填校验。
 * 不记录报文、密钥或签名源串日志；边界异常由调用方按接口错误规范处理。
 */
public final class HmacSm3SignUtils {
    private static final int KEY_BYTES = 32;
    private static final String SIGN_FIELD = "sign";
    private static final String KEY_ID_FIELD = "keyId";
    private static final Pattern KEY_FORMAT = Pattern.compile("[A-Za-z0-9_-]{43}");
    private static final Pattern SIGN_FORMAT = Pattern.compile("[0-9a-f]{64}");

    private HmacSm3SignUtils() { }

    /** 固定字段范围；枚举名称仅用于本地选字段，不拼入签名源串或发送给平台。 */
    public enum Operation {
        CREATE(false, "account", "nonce_str", "notify_url", "num", "out_order_sn", "product_id", "timestamp"),
        QUERY(false, "nonce_str", "out_order_sn", "timestamp"),
        INVALID(false, "nonce_str", "out_order_sn", "timestamp"),
        ORDER_CALLBACK(true, "account", "keyId", "nonce_str", "order_id", "order_status", "out_order_sn",
                "timestamp", "total_price", "total_settle_price", "type"),
        WRITEOFF_CALLBACK(true, "account", "invalid_time", "keyId", "nonce_str", "order_id", "out_order_sn",
                "state_type", "timestamp", "use_state", "use_time");

        private final boolean callback;
        private final Set<String> fields;

        Operation(boolean callback, String... fields) {
            this.callback = callback;
            // 字段名均为ASCII；自然排序与协议的ASCII升序一致，不依赖Map输入顺序。
            this.fields = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(fields)));
        }
    }

    /**
     * 构造 stringA，供签名及离线排错使用；返回值可能含个人信息，禁止直接写入生产日志。
     * 请求忽略未定义字段以及缺失/null/空字符串，保留0；回调保留全部定义字段的空字符串。
     * 回调缺字段、JSON null或额外字段没有约定安全语义，本工具拒绝隐式补值或忽略。
     * 字符串保持原值，不能trim、URL编码、重新转义或添加operation前缀。
     */
    public static String canonicalize(Operation operation, Map<String, ?> payload) {
        requireInputs(operation, payload);
        if (operation.callback) {
            for (String name : payload.keySet()) {
                if (!SIGN_FIELD.equals(name) && !operation.fields.contains(name)) {
                    throw new IllegalArgumentException("回调包含协议未定义字段");
                }
            }
        }
        StringJoiner source = new StringJoiner(",");
        for (String name : operation.fields) {
            Object value = payload.get(name);
            if (value == null) {
                if (operation.callback) {
                    throw new IllegalArgumentException("回调定义字段必须存在且不能为null");
                }
                continue;
            }
            String text = scalar(value);
            if (operation.callback && KEY_ID_FIELD.equals(name) && text.isEmpty()) {
                throw new IllegalArgumentException("回调keyId不能为空");
            }
            if (operation.callback || !text.isEmpty()) {
                source.add(name + "=" + text);
            }
        }
        return source.toString();
    }

    /**
     * 使用完整32字节密钥计算64位小写十六进制认证码，不修改输入Map。
     * 下单、查单、作废分别选择CREATE、QUERY、INVALID，完成后把返回值写入payload.sign。
     * 支持回调类型用于协议自测或平台实现；商户接收通知应使用verifyCallback。
     */
    public static String sign(Operation operation, Map<String, ?> payload, String signKey) {
        return Hex.toHexString(mac(canonicalize(operation, payload), signKey));
    }

    /**
     * 仅验证密码学认证码，不验证商户及订单归属。
     * sign缺失、格式错误或不匹配返回false；密钥或字段结构非法抛IllegalArgumentException。
     * 不接受大写、截断或Base64认证码，不回退旧SM3/aesKey算法。
     */
    public static boolean verify(Operation operation, Map<String, ?> payload, String signKey) {
        requireInputs(operation, payload);
        Object sign = payload.get(SIGN_FIELD);
        if (!(sign instanceof String) || !SIGN_FORMAT.matcher((String) sign).matches()) {
            return false;
        }
        byte[] expected = mac(canonicalize(operation, payload), signKey);
        byte[] received = Hex.decode((String) sign);
        return MessageDigest.isEqual(expected, received);
    }

    /**
     * 商户回调密码学验签入口，参见《交行直充业务系统详细设计》3.1。
     * 调用方先从可信配置确定商户范围，再按报文keyId查该商户未撤销的密钥；
     * trustedKeyId和signKey必须来自这条可信记录，未知/撤销/跨商户密钥直接拒绝，不试最新密钥。
     * 订单不绑定密钥；普通token刷新不更换商户密钥，token过期也不影响回调验签。
     * 验签成功后仍须独立确认本商户本地订单身份、业务字段、幂等和状态迁移。
     */
    public static boolean verifyCallback(Operation operation, Map<String, ?> payload,
                                         String trustedKeyId, String signKey) {
        requireInputs(operation, payload);
        if (!operation.callback) {
            throw new IllegalArgumentException("verifyCallback仅用于两类回调");
        }
        if (trustedKeyId == null || trustedKeyId.isEmpty()
                || !trustedKeyId.equals(payload.get(KEY_ID_FIELD))) {
            return false;
        }
        return verify(operation, payload, signKey);
    }

    private static void requireInputs(Operation operation, Map<String, ?> payload) {
        if (operation == null || payload == null) {
            throw new IllegalArgumentException("operation和payload不能为空");
        }
    }

    private static String scalar(Object value) {
        if (value instanceof String || value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long || value instanceof BigInteger) {
            return value.toString();
        }
        throw new IllegalArgumentException("签名字段仅接受字符串或整数，不能隐式转换小数/布尔/集合");
    }

    private static byte[] mac(String source, String signKey) {
        if (signKey == null || !KEY_FORMAT.matcher(signKey).matches()) {
            throw new IllegalArgumentException("signKey必须为43字符无填充Base64URL编码");
        }
        byte[] key = Base64.getUrlDecoder().decode(signKey);
        try {
            // 重新编码校验未使用的尾部bit，拒绝能解出相同字节的非规范编码。
            if (key.length != KEY_BYTES || !Base64.getUrlEncoder().withoutPadding().encodeToString(key).equals(signKey)) {
                throw new IllegalArgumentException("signKey必须规范编码完整32字节密钥");
            }
            HMac hmac = new HMac(new SM3Digest());
            hmac.init(new KeyParameter(key));
            byte[] message = source.getBytes(StandardCharsets.UTF_8);
            hmac.update(message, 0, message.length);
            byte[] result = new byte[hmac.getMacSize()];
            hmac.doFinal(result, 0);
            return result;
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }
}

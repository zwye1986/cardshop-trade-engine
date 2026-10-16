package com.cardshop.trade.engine.application.v1.crypto;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static com.cardshop.trade.engine.application.v1.crypto.V12MessageSignatures.Operation.*;

class V12MessageSignaturesTest {
    static final String KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    static final ObjectMapper JSON = new ObjectMapper();
    static JsonNode vectors() throws Exception {
        try (java.io.InputStream in = V12MessageSignaturesTest.class.getResourceAsStream("/signature-vectors.json")) {
            return JSON.readTree(in).get("vectors");
        }
    }
    static Map<String, Object> payload(int index) throws Exception {
        return JSON.convertValue(vectors().get(index).get("payload"), new TypeReference<Map<String,Object>>() {});
    }

    @Test void matchesAllSixWordVectors() throws Exception {
        for (JsonNode v : vectors()) {
            V12MessageSignatures.Operation op = V12MessageSignatures.Operation.valueOf(v.get("operation").asText());
            Map<String,Object> p = JSON.convertValue(v.get("payload"), new TypeReference<Map<String,Object>>() {});
            assertEquals(v.get("stringA").asText(), V12MessageSignatures.canonicalize(op, p));
            assertEquals(v.get("expectedSign").asText(), V12MessageSignatures.sign(op, p, KEY));
            p.put("sign", v.get("expectedSign").asText());
            assertTrue(V12MessageSignatures.verify(op, p, KEY));
        }
    }
    @Test void requestUsesWhitelistAndSkipsEmptyButKeepsZeroAndWhitespace() {
        Map<String,Object> p = new HashMap<>();
        p.put("account", " 中文,a=b "); p.put("num", 0); p.put("notify_url", "");
        p.put("timestamp", null); p.put("product_id", "001");
        for (String name : Arrays.asList("sign", "aesKey", "signKey", "token", "app_secret", "keyId", "extra")) p.put(name, "ignored");
        assertEquals("account= 中文,a=b ,num=0,product_id=001", V12MessageSignatures.canonicalize(CREATE,p));
    }
    @Test void rejectsUnsupportedValuesInsteadOfImplicitConversion() {
        for (Object value : Arrays.asList(true, 1.0, new BigDecimal("1.0"), Arrays.asList(1), new HashMap<>())) {
            assertThrows(IllegalArgumentException.class, () -> V12MessageSignatures.canonicalize(CREATE, Collections.singletonMap("num",value)));
        }
    }
    @Test void validatesCanonicalFullLengthBase64UrlKey() throws Exception {
        Map<String,Object> p=payload(2);
        for (String key : Arrays.asList(null, "", KEY+"=", KEY.substring(1), KEY.substring(0,42)+"9", KEY.replace('_','/')+" ")) {
            assertThrows(IllegalArgumentException.class, () -> V12MessageSignatures.sign(QUERY,p,key));
        }
        byte[] wrong = new byte[32]; Arrays.fill(wrong, (byte) 5);
        assertFalse(V12MessageSignatures.verify(QUERY,p,Base64.getUrlEncoder().withoutPadding().encodeToString(wrong)));
    }
    @Test void usesAll32KeyBytes() throws Exception {
        byte[] changed=Base64.getUrlDecoder().decode(KEY); changed[31]^=1;
        assertNotEquals(V12MessageSignatures.sign(QUERY,payload(2),KEY),
                V12MessageSignatures.sign(QUERY,payload(2),Base64.getUrlEncoder().withoutPadding().encodeToString(changed)));
    }
    @Test void rejectsTamperingAndMalformedSignatures() throws Exception {
        Map<String,Object> p=payload(4);
        p.put("total_price",1951); assertFalse(V12MessageSignatures.verify(ORDER_CALLBACK,p,KEY));
        p=payload(4);
        for (Object sign : Arrays.asList(null, "", "abc", 123, p.get("sign").toString().toUpperCase(Locale.ROOT), String.join("",Collections.nCopies(64,"g")))) {
            p.put("sign",sign); assertFalse(V12MessageSignatures.verify(ORDER_CALLBACK,p,KEY));
        }
    }
    @Test void callbacksRetainEmptyAndRejectMissingOrNullFields() throws Exception {
        Map<String,Object> p=payload(5);
        assertNotNull(V12MessageSignatures.canonicalize(WRITEOFF_CALLBACK,p));
        assertTrue(V12MessageSignatures.canonicalize(WRITEOFF_CALLBACK,p).contains("invalid_time=,keyId="));
        p.remove("invalid_time");
        assertThrows(IllegalArgumentException.class, () -> V12MessageSignatures.canonicalize(WRITEOFF_CALLBACK,p));
        p.put("invalid_time",null);
        assertThrows(IllegalArgumentException.class, () -> V12MessageSignatures.canonicalize(WRITEOFF_CALLBACK,p));
    }
    @Test void callbackMustMatchScopedTrustedKeyWithoutOrderKeyBinding() throws Exception {
        Map<String,Object> p=payload(4);
        String id=p.get("keyId").toString();
        assertTrue(V12MessageSignatures.verifyCallback(ORDER_CALLBACK,p,id,KEY));
        assertFalse(V12MessageSignatures.verifyCallback(ORDER_CALLBACK,p,"new-key",KEY));
        assertFalse(V12MessageSignatures.verifyCallback(ORDER_CALLBACK,p,null,KEY));
        assertThrows(IllegalArgumentException.class, () -> V12MessageSignatures.verifyCallback(QUERY,p,id,KEY));
        p.put("keyId","tampered"); assertFalse(V12MessageSignatures.verify(ORDER_CALLBACK,p,KEY));
    }
    @Test void callbackUnknownFieldsAreRejectedToAvoidConsumingUnsignedData() throws Exception {
        Map<String,Object> p=payload(4); p.put("extra","unsigned");
        assertThrows(IllegalArgumentException.class, () -> V12MessageSignatures.verify(ORDER_CALLBACK,p,KEY));
    }
    @Test void inputMapsAreNotMutated() throws Exception {
        Map<String,Object> p=payload(1), original=new HashMap<>(p);
        V12MessageSignatures.sign(CREATE,Collections.unmodifiableMap(p),KEY);
        assertEquals(original,p);
    }
}

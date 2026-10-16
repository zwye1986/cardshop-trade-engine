package com.cardshop.trade.engine.api.v1;

import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import com.cardshop.trade.engine.application.v1.crypto.*;
import java.util.*;

/** 公开协议固定向量及仅用于测试的商户密钥仓库，参见详细设计3.1。 */
public final class V12TestCredentials {
    public static final String KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    public static final V12SigningCredential CREDENTIAL = new V12SigningCredential("test-key", "A36", KEY);

    public static V1MerchantCommand command(String app, String org, String body) {
        return command(app, org, body, "");
    }
    public static V1MerchantCommand command(String app, String org, String body, String header) {
        return new V1MerchantCommand(app, org, body, header, CREDENTIAL);
    }
    public static String signSource(String source) {
        Map<String,Object> fields = new TreeMap<>();
        for (String part : source.split(",")) {
            String[] pair = part.split("=", 2);
            fields.put(pair[0], pair[1]);
        }
        return V12MessageSignatures.sign(fields.containsKey("account")
                ? V12MessageSignatures.Operation.CREATE : V12MessageSignatures.Operation.QUERY, fields, KEY);
    }

    public static class Store implements V12SigningCredentialStorePort {
        private final Map<String,V12SigningCredential> keys = new HashMap<>();
        private final Map<String,String> current = new HashMap<>();
        private final Set<String> revoked = new HashSet<>();

        public synchronized void save(V12SigningCredential key) {
            keys.put(key.keyId(), key);
            current.put(key.orgCode(), key.keyId());
        }
        public synchronized V12SigningCredential getOrCreateCurrent(String org) {
            if (!current.containsKey(org)) save(V12SigningCredential.generate(org));
            return findCurrent(org).orElseThrow(() -> new IllegalStateException("revoked current key"));
        }
        public synchronized Optional<V12SigningCredential> findCurrent(String org) {
            return findForCallback(org, current.get(org));
        }
        public synchronized Optional<V12SigningCredential> findForCallback(String org, String id) {
            return Optional.ofNullable(keys.get(id))
                    .filter(k -> k.orgCode().equals(org) && !revoked.contains(id));
        }
        public synchronized V12SigningCredential rotate(String org) {
            V12SigningCredential key = V12SigningCredential.generate(org);
            save(key);
            return key;
        }
        public synchronized boolean revoke(String org, String id) {
            return findForCallback(org, id).map(k -> revoked.add(id)).orElse(false);
        }
    }
}

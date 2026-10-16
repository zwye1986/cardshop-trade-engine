package com.cardshop.trade.engine.api.v1;

import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import com.cardshop.trade.engine.application.v1.crypto.*;
import java.time.Instant;
import java.util.*;

/** 公开协议固定向量，仅用于单测；不依赖生产凭证。 */
public final class V12TestCredentials {
    public static final String KEY="AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    public static final V12SigningCredential CREDENTIAL=new V12SigningCredential("test-key","app-gray","A36",KEY,Instant.parse("2100-01-01T00:00:00Z"));
    public static V1MerchantCommand command(String app,String org,String body) { return command(app,org,body,""); }
    public static V1MerchantCommand command(String app,String org,String body,String header) {
        return new V1MerchantCommand(app,org,body,header,CREDENTIAL);
    }
    public static String signSource(String source) {
        Map<String,Object> fields=new TreeMap<>();
        for (String part:source.split(",")) { String[] pair=part.split("=",2); fields.put(pair[0],pair[1]); }
        return V12MessageSignatures.sign(fields.containsKey("account") ? V12MessageSignatures.Operation.CREATE : V12MessageSignatures.Operation.QUERY,fields,KEY);
    }
    public static class Store implements V12SigningCredentialStorePort {
        public final Map<String,V12SigningCredential> keys=new HashMap<>();
        public void save(V12SigningCredential credential) { keys.put(credential.keyId(),credential); }
        public Optional<V12SigningCredential> findActive(String app,String org,String id) {
            return Optional.ofNullable(keys.get(id)).filter(k -> k.appId().equals(app) && k.orgCode().equals(org));
        }
        public Optional<V12SigningCredential> findForOrder(String org,String order) { return Optional.empty(); }
        public boolean revoke(String org,String id) { return keys.computeIfPresent(id,(ignored,k) -> k.orgCode().equals(org) ? null : k)==null; }
    }
}

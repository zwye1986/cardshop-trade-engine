package com.cardshop.trade.engine.simulator.bocom;

import com.bocom.api.utils.BocomSignature;
import com.bocom.api.utils.WebUtils;
import com.bocom.api.utils.enums.EncryptType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** 独立银行端验签和幂等证据；参见详细设计「交行 2104 通知实施细化」。 */
class Bocom2104GatewayServerTest {
    @Test void verifiesBothAlgorithmsAndCorrelatesRetriesWithoutRepeatingBusinessEvent() throws Exception {
        for (String algorithm : List.of("RSA", "SM2")) {
            var merchant = Bocom2104GatewayServer.generateKeys(algorithm);
            var bank = Bocom2104GatewayServer.generateKeys(algorithm);
            try (var server = new Bocom2104GatewayServer("offline-app", algorithm, merchant.publicKey(), bank.privateKey())) {
                server.start();
                String text=new ObjectMapper().writeValueAsString(Map.of("operate_type","2","order_id","O-SYNTHETIC",
                        "out_order_sn","OUT-SYNTHETIC","use_time","2026-10-21 11:12:13","invalid_time","",
                        "account","中😀,quote\"slash\\line\n"));
                var first=send(server,merchant.privateKey(),algorithm,text,"msg-1");
                var second=send(server,merchant.privateKey(),algorithm,text,"msg-2");
                assertThat(first.statusCode()).as(server.failures().toString()).isEqualTo(200);
                assertThat(second.statusCode()).isEqualTo(200);
                var response=new ObjectMapper().readTree(first.body());
                String biz=response.get("rsp_biz_content").toString();
                assertThat(BocomSignature.verify(type(algorithm),biz,bank.publicKey(),"UTF-8",response.get("sign").asText())).isTrue();
                assertThat(server.observations()).hasSize(2);
                assertThat(server.observations().getFirst().fields()).containsEntry("use_time","2026-10-21 11:12:13")
                        .containsEntry("invalid_time","").containsEntry("account","中😀,quote\"slash\\line\n");
                assertThat(server.find("msg-2")).isPresent();
                assertThat(server.businessEventCount()).isEqualTo(1);
                var wrong=Bocom2104GatewayServer.generateKeys(algorithm);
                assertThat(send(server,wrong.privateKey(),algorithm,text,"forged").statusCode()).isEqualTo(400);
                assertThat(server.observations()).hasSize(2);
            }
        }
    }
    @Test void exposesControlledFailureModesAndRejectsDuplicateFields() throws Exception {
        var keys=Bocom2104GatewayServer.generateKeys("RSA");
        try(var server=new Bocom2104GatewayServer("offline-app","RSA",keys.publicKey(),keys.privateKey())) {
            server.start();
            String text="{\"operate_type\":\"1\",\"order_id\":\"O\",\"out_order_sn\":\"OUT\",\"total_price\":1950,\"total_settle_price\":1877}";
            server.mode(Bocom2104GatewayServer.Mode.BUSINESS_FAILURE);
            assertThat(send(server,keys.privateKey(),"RSA",text,"failed").body()).as(server.failures().toString()).contains("500");
            server.mode(Bocom2104GatewayServer.Mode.HTTP_FAILURE);
            assertThat(send(server,keys.privateKey(),"RSA",text,"http").statusCode()).isEqualTo(500);
            assertThat(server.observations().getFirst().fields()).containsEntry("total_price","1950").containsEntry("total_settle_price","1877");
            for(String invalid:List.of("{\"order_id\":\"O\",\"order_id\":\"OTHER\"}","order_id:O","[]","null",
                    "{\"account\":null}","{\"account\":{}}","{\"account\":true}","{\"order_id\":\"O\"} trailing")) {
                assertThatThrownBy(()->Bocom2104GatewayServer.parseFields(invalid)).isInstanceOf(IllegalArgumentException.class);
            }
        }
    }
    private HttpResponse<String> send(Bocom2104GatewayServer server,String privateKey,String algorithm,String text,String msgId) throws Exception {
        String biz=new ObjectMapper().writeValueAsString(Map.of("head",Map.of(),"body",Map.of("trx_data_imprt",text,"protocol","offline-contract")));
        var form=new TreeMap<String,String>();form.put("app_id","offline-app");form.put("charset","UTF-8");form.put("fmt_type","json");
        form.put("msg_id",msgId);form.put("timestamp","2026-10-21 11:12:13");form.put("biz_content",biz);
        form.put("sign",BocomSignature.sign(type(algorithm),WebUtils.buildOrderedSignStr(Bocom2104GatewayServer.API_PATH,form),privateKey,"UTF-8"));
        String encoded=form.entrySet().stream().map(e->URLEncoder.encode(e.getKey(),StandardCharsets.UTF_8)+"="+URLEncoder.encode(e.getValue(),StandardCharsets.UTF_8)).collect(java.util.stream.Collectors.joining("&"));
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(server.url())).header("Content-Type","application/x-www-form-urlencoded;charset=UTF-8").POST(HttpRequest.BodyPublishers.ofString(encoded)).build(),HttpResponse.BodyHandlers.ofString());
    }
    private EncryptType type(String algorithm) {return "RSA".equals(algorithm)?EncryptType.RSA_AND_AES:EncryptType.SM2_AND_SM4;}
}

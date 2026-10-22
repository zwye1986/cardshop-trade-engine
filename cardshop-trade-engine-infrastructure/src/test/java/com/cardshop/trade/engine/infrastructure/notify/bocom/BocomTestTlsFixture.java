package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyStore;
import java.util.*;
import javax.net.ssl.*;

/**
 * 同一测试 JVM 共用一个临时信任锚，严格遵守生产 SDK 工厂的全局证书约束。
 * 参见详细设计「交行 2104 通知实施细化」；不反射重置工厂、不削弱 TLS。
 */
final class BocomTestTlsFixture {
    private static final char[] PASSWORD="offline-test-password".toCharArray();
    private static final Path DIRECTORY;
    private static final Path TRUSTED;
    static {
        try {DIRECTORY=Files.createTempDirectory("bocom-test-tls-");TRUSTED=createCertificate("trusted",null);}
        catch(Exception exception) {throw new ExceptionInInitializerError(exception);}
    }
    private BocomTestTlsFixture() { }
    static Path trustedCertificate() {return TRUSTED;}
    static Path directory() {return DIRECTORY;}
    static Path createCertificate(String name,String startDate) throws Exception {
        var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/keytool").toString(),"-genkeypair","-alias","server",
                "-keyalg","RSA","-keysize","2048","-dname","CN=localhost","-ext","SAN=dns:localhost","-validity","1",
                "-keystore",DIRECTORY.resolve(name+".p12").toString(),"-storetype","PKCS12","-storepass",new String(PASSWORD),"-keypass",new String(PASSWORD),"-noprompt"));
        if(startDate!=null) {command.add("-startdate");command.add(startDate);}
        var process=new ProcessBuilder(command).redirectErrorStream(true).start();
        String output=new String(process.getInputStream().readAllBytes(),StandardCharsets.UTF_8);
        if(process.waitFor()!=0) throw new IllegalStateException("test keytool failed: "+output);
        Path certificate=DIRECTORY.resolve(name+".cer");Files.write(certificate,store(name).getCertificate("server").getEncoded());
        DIRECTORY.resolve(name+".p12").toFile().deleteOnExit();certificate.toFile().deleteOnExit();
        return certificate;
    }
    private static KeyStore store(String name) throws Exception {
        var store=KeyStore.getInstance("PKCS12");
        try(var input=Files.newInputStream(DIRECTORY.resolve(name+".p12"))) {store.load(input,PASSWORD);}
        return store;
    }
    static HttpsServer server(String certificateName) throws Exception {
        var managers=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());managers.init(store(certificateName),PASSWORD);
        var context=SSLContext.getInstance("TLS");context.init(managers.getKeyManagers(),null,null);
        var server=HttpsServer.create(new InetSocketAddress("localhost",0),0);server.setHttpsConfigurator(new HttpsConfigurator(context));
        return server;
    }
}

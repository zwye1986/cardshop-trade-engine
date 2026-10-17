package com.cardshop.trade.engine.application.recharge;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 名创核销接收用例：先认证再持久提交，不发送商户HTTP；参见docs/交行直充业务系统详细设计.md「9.5」。 */
public final class ReceiveMinisoWriteOffService {
    private final MinisoWriteOffAuthenticator authenticator;
    private final MinisoWriteOffStore store;
    private final MinisoWriteOffObserver observer;

    public ReceiveMinisoWriteOffService(
            MinisoWriteOffAuthenticator authenticator, MinisoWriteOffStore store) {
        this(authenticator, store, MinisoWriteOffObserver.NOOP);
    }

    public ReceiveMinisoWriteOffService(
            MinisoWriteOffAuthenticator authenticator,
            MinisoWriteOffStore store,
            MinisoWriteOffObserver observer) {
        this.authenticator = Objects.requireNonNull(authenticator);
        this.store = Objects.requireNonNull(store);
        this.observer = Objects.requireNonNull(observer);
    }

    public void receive(String rawBody, Map<String, List<String>> headers) {
        try {
            store.accept(authenticator.authenticate(rawBody, headers));
        } catch (MinisoWriteOffException ex) {
            observer.event(
                    switch (ex.reason()) {
                        case INVALID -> "invalid";
                        case SIGNATURE_FAILED -> "signature_failed";
                        case CONFLICT -> "conflict";
                        default -> "storage_failed";
                    });
            throw ex;
        }
    }
}

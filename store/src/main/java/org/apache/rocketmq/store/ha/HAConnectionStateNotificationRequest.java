package org.apache.rocketmq.store.ha;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.CompletableFuture;

@Getter
@Setter
public class HAConnectionStateNotificationRequest {

    private final CompletableFuture<Boolean> requestFuture = new CompletableFuture<>();

    private final HAConnectionState expectState;

    private final String remoteAddr;

    private final boolean notifyWhenShutdown;

    public HAConnectionStateNotificationRequest(HAConnectionState expectState, String remoteAddr, boolean notifyWhenShutdown) {
        this.expectState = expectState;
        this.remoteAddr = remoteAddr;
        this.notifyWhenShutdown = notifyWhenShutdown;
    }

}
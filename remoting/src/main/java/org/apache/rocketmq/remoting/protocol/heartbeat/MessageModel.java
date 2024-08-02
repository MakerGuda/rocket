package org.apache.rocketmq.remoting.protocol.heartbeat;

import lombok.Getter;

/**
 * 消息模式
 */
@Getter
public enum MessageModel {

    /**
     * 广播
     */
    BROADCASTING("BROADCASTING"),
    /**
     * 集群
     */
    CLUSTERING("CLUSTERING");

    private final String modeCN;

    MessageModel(String modeCN) {
        this.modeCN = modeCN;
    }

}
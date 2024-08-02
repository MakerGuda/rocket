package org.apache.rocketmq.remoting.protocol.heartbeat;

import lombok.Getter;

/**
 * 消费类型
 */
@Getter
public enum ConsumeType {

    CONSUME_ACTIVELY("PULL"),

    CONSUME_PASSIVELY("PUSH"),

    CONSUME_POP("POP");

    private final String typeCN;

    ConsumeType(String typeCN) {
        this.typeCN = typeCN;
    }

}
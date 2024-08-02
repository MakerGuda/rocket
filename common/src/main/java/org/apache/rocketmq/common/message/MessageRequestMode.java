package org.apache.rocketmq.common.message;

import lombok.Getter;

/**
 * 消息请求方式
 */
@Getter
public enum MessageRequestMode {

    /**
     * pull
     */
    PULL("PULL"),

    /**
     * pop, consumer working in pop mode could share MessageQueue
     */
    POP("POP");

    private final String name;

    MessageRequestMode(String name) {
        this.name = name;
    }

}
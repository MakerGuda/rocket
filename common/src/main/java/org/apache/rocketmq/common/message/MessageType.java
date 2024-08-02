package org.apache.rocketmq.common.message;

import lombok.Getter;

/**
 * 消息类型
 */
@Getter
public enum MessageType {

    /**
     * 普通消息
     */
    Normal_Msg("Normal"),
    /**
     * 半事务消息
     */
    Trans_Msg_Half("Trans"),
    /**
     * 事务提交消息
     */
    Trans_msg_Commit("TransCommit"),
    /**
     * 延迟消息
     */
    Delay_Msg("Delay"),
    /**
     * 顺序消息
     */
    Order_Msg("Order");

    private final String shortName;

    MessageType(String shortName) {
        this.shortName = shortName;
    }

}
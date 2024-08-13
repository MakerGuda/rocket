package org.apache.rocketmq.broker.client;

/**
 * 消费者组事件
 */
public enum ConsumerGroupEvent {

    /**
     * 消费者组内的一些消费者发生改变
     */
    CHANGE,
    /**
     * 消费者组注销
     */
    UNREGISTER,
    /**
     * 消费者注册
     */
    REGISTER,
    /**
     * 新的消费者客户端注册
     */
    CLIENT_REGISTER,
    /**
     * 消费者客户端取消注册
     */
    CLIENT_UNREGISTER

}
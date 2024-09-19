package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;

/**
 * 推模式消息消费
 */
public interface MQPushConsumer extends MQConsumer {

    /**
     * 启动消费者
     */
    void start() throws MQClientException;

    /**
     * 关闭消费者
     */
    void shutdown();

    /**
     * 注册消息事件监听器
     */
    void registerMessageListener(MessageListener messageListener);

    /**
     * 注册并发消息事件监听器
     */
    void registerMessageListener(final MessageListenerConcurrently messageListener);

    /**
     * 注册顺序消息事件监听器
     */
    void registerMessageListener(final MessageListenerOrderly messageListener);

    /**
     * 基于主题订阅消息，指定订阅关系，如 tag1 || tag2 || tag3
     */
    void subscribe(final String topic, final String subExpression) throws MQClientException;

    /**
     * 订阅主题
     */
    void subscribe(final String topic, final String fullClassName, final String filterClassSource) throws MQClientException;

    /**
     * 订阅主题，并指定消息选择器
     */
    void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

    /**
     * 取消订阅主题
     */
    void unsubscribe(final String topic);

    /**
     * 暂停消费者
     */
    void suspend();

    /**
     * 恢复消费者
     */
    void resume();

}
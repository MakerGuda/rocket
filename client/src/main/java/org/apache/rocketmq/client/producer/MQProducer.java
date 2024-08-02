package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Collection;
import java.util.List;

/**
 * mq生产者通用接口
 */
public interface MQProducer extends MQAdmin {

    /**
     * 启动生产者
     */
    void start() throws MQClientException;

    /**
     * 关闭生产者
     */
    void shutdown();

    /**
     * 获取主题下的队列列表
     */
    List<MessageQueue> fetchPublishMessageQueues(final String topic) throws MQClientException;

    /**
     * 同步发送消息
     */
    SendResult send(final Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 同步发送消息，带超时时间
     */
    SendResult send(final Message msg, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步发送消息
     */
    void send(final Message msg, final SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException, MQBrokerException;

    /**
     * 异步发送消息
     */
    void send(final Message msg, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 单向发送消息
     */
    void sendOneway(final Message msg) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 同步发送消息到指定mq
     */
    SendResult send(final Message msg, final MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 同步发送消息到指定mq
     */
    SendResult send(final Message msg, final MessageQueue mq, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步发送消息到指定mq
     */
    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 异步发送消息到指定mq
     */
    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 单向发送消息到指定mq
     */
    void sendOneway(final Message msg, final MessageQueue mq) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 指定mq选择器，同步发送消息
     */
    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 指定mq选择器，同步发送消息
     */
    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 指定mq选择弃，异步发送消息
     */
    void send(final Message msg, final MessageQueueSelector selector, final Object arg, final SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 指定mq选择器，异步发送消息
     */
    void send(final Message msg, final MessageQueueSelector selector, final Object arg, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 指定mq选择器，单向发送消息
     */
    void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 同步发送事务消息
     */
    TransactionSendResult sendMessageInTransaction(final Message msg, final Object arg) throws MQClientException;

    /**
     * 同步发送批量消息
     */
    SendResult send(final Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 同步发送批量消息
     */
    SendResult send(final Collection<Message> msgs, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 指定mq，同步发送批量消息
     */
    SendResult send(final Collection<Message> msgs, final MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 指定mq，同步发送批量消息
     */
    SendResult send(final Collection<Message> msgs, final MessageQueue mq, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步发送批量消息
     */
    void send(final Collection<Message> msgs, final SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步发送批量消息
     */
    void send(final Collection<Message> msgs, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 指定mq，异步发送批量消息
     */
    void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 指定mq，异步发送批量消息
     */
    void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final RequestCallback requestCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final MessageQueueSelector selector, final Object arg, final RequestCallback requestCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueue mq, final long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

}
package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Set;

/**
 * 消息队列消费接口
 */
public interface MQConsumer extends MQAdmin {

    /**
     * 消息消费失败后，将消息发送回broker，待一定延迟级别后继续消费
     */
    void sendMessageBack(final MessageExt msg, final int delayLevel) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * 消息消费失败后，将消息发送回broker，待一定延迟级别后继续消费
     */
    void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * 获取当前消费者对主题，分配了哪些队列
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;

}
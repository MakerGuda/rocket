package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Set;

/**
 *消费者内部接口
 */
public interface MQConsumerInner {

    String groupName();

    MessageModel messageModel();

    ConsumeType consumeType();

    ConsumeFromWhere consumeFromWhere();

    /**
     * 获取消费者的订阅关系
     */
    Set<SubscriptionData> subscriptions();

    /**
     * 消费者组内重平衡
     */
    void doRebalance();

    boolean tryRebalance();

    void persistConsumerOffset();

    /**
     * 更新消费者主题订阅信息
     */
    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    /**
     * 判断消费者的主题订阅数据是否需要更新
     */
    boolean isSubscribeTopicNeedUpdate(final String topic);

    boolean isUnitMode();

    ConsumerRunningInfo consumerRunningInfo();

}
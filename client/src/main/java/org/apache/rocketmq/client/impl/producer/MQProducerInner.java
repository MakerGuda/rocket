package org.apache.rocketmq.client.impl.producer;

import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

import java.util.Set;

public interface MQProducerInner {

    /**
     * 获取生产者发布的主题列表
     */
    Set<String> getPublishTopicList();

    /**
     * 判断当前生产者的主题路由信息是否需要更新
     */
    boolean isPublishTopicNeedUpdate(final String topic);

    TransactionCheckListener checkListener();

    TransactionListener getCheckListener();

    void checkTransactionState(final String addr, final MessageExt msg, final CheckTransactionStateRequestHeader checkRequestHeader);

    /**
     * 更新主题发布信息
     */
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    boolean isUnitMode();

}
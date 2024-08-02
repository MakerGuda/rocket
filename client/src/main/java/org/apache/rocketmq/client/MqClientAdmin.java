package org.apache.rocketmq.client;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.body.*;
import org.apache.rocketmq.remoting.protocol.header.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface MqClientAdmin {

    CompletableFuture<List<MessageExt>> queryMessage(String address, boolean uniqueKeyFlag, boolean decompressBody, QueryMessageRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<List<QueueTimeSpan>> queryConsumeTimeSpan(String address, QueryConsumeTimeSpanRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<Void> deleteTopicInBroker(String address, DeleteTopicRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<Void> deleteSubscriptionGroup(String address, DeleteSubscriptionGroupRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<MessageExt> viewMessage(String address, ViewMessageRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumerConnection> getConsumerConnectionList(String address, GetConsumerConnectionListRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<TopicList> queryTopicsByConsumer(String address, QueryTopicsByConsumerRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumeStats> getConsumeStats(String address, GetConsumeStatsRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<GroupList> queryTopicConsumeByWho(String address, QueryTopicConsumeByWhoRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumerRunningInfo> getConsumerRunningInfo(String address, GetConsumerRunningInfoRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumeMessageDirectlyResult> consumeMessageDirectly(String address, ConsumeMessageDirectlyResultRequestHeader requestHeader, long timeoutMillis);

}
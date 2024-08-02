package org.apache.rocketmq.client.impl.admin;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.MqClientAdmin;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.body.*;
import org.apache.rocketmq.remoting.protocol.header.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MqClientAdminImpl implements MqClientAdmin {

    private final static Logger log = LoggerFactory.getLogger(MqClientAdminImpl.class);

    private final RemotingClient remotingClient;

    public MqClientAdminImpl(RemotingClient remotingClient) {
        this.remotingClient = remotingClient;
    }

    @Override
    public CompletableFuture<List<MessageExt>> queryMessage(String address, boolean uniqueKeyFlag, boolean decompressBody, QueryMessageRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<List<MessageExt>> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
        request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, String.valueOf(uniqueKeyFlag));
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                List<MessageExt> wrappers = MessageDecoder.decodesBatch(ByteBuffer.wrap(response.getBody()), true, decompressBody, true);
                future.complete(filterMessages(wrappers, requestHeader.getTopic(), requestHeader.getKey(), uniqueKeyFlag));
            } else if (response.getCode() == ResponseCode.QUERY_NOT_FOUND)  {
                List<MessageExt> wrappers = new ArrayList<>();
                future.complete(wrappers);
            } else {
                log.warn("queryMessage getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<List<QueueTimeSpan>> queryConsumeTimeSpan(String address, QueryConsumeTimeSpanRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<List<QueueTimeSpan>> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                QueryConsumeTimeSpanBody consumeTimeSpanBody = GroupList.decode(response.getBody(), QueryConsumeTimeSpanBody.class);
                future.complete(consumeTimeSpanBody.getConsumeTimeSpanSet());
            } else {
                log.warn("queryConsumerTimeSpan getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteTopicInBroker(String address, DeleteTopicRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                future.complete(null);
            } else {
                log.warn("deleteTopicInBroker getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteSubscriptionGroup(String address, DeleteSubscriptionGroupRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                future.complete(null);
            } else {
                log.warn("deleteSubscriptionGroup getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<MessageExt> viewMessage(String address, ViewMessageRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<MessageExt> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);
                future.complete(messageExt);
            } else {
                log.warn("viewMessage getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ConsumerConnection> getConsumerConnectionList(String address, GetConsumerConnectionListRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumerConnection> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ConsumerConnection consumerConnection = ConsumerConnection.decode(response.getBody(), ConsumerConnection.class);
                future.complete(consumerConnection);
            } else {
                log.warn("getConsumerConnectionList getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<TopicList> queryTopicsByConsumer(String address, QueryTopicsByConsumerRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<TopicList> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPICS_BY_CONSUMER, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                future.complete(topicList);
            } else {
                log.warn("queryTopicsByConsumer getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ConsumeStats> getConsumeStats(String address, GetConsumeStatsRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumeStats> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ConsumeStats consumeStats = ConsumeStats.decode(response.getBody(), ConsumeStats.class);
                future.complete(consumeStats);
            } else {
                log.warn("getConsumeStats getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<GroupList> queryTopicConsumeByWho(String address, QueryTopicConsumeByWhoRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<GroupList> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                GroupList groupList = GroupList.decode(response.getBody(), GroupList.class);
                future.complete(groupList);
            } else {
                log.warn("queryTopicConsumeByWho getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ConsumerRunningInfo> getConsumerRunningInfo(String address, GetConsumerRunningInfoRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumerRunningInfo> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ConsumerRunningInfo info = ConsumerRunningInfo.decode(response.getBody(), ConsumerRunningInfo.class);
                future.complete(info);
            } else {
                log.warn("getConsumerRunningInfo getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<ConsumeMessageDirectlyResult> consumeMessageDirectly(String address, ConsumeMessageDirectlyResultRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumeMessageDirectlyResult> future = new CompletableFuture<>();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);
        remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
            if (response.getCode() == ResponseCode.SUCCESS) {
                ConsumeMessageDirectlyResult info = ConsumeMessageDirectlyResult.decode(response.getBody(), ConsumeMessageDirectlyResult.class);
                future.complete(info);
            } else {
                log.warn("consumeMessageDirectly getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                future.completeExceptionally(new MQClientException(response.getCode(), response.getRemark()));
            }
        });
        return future;
    }

    private List<MessageExt> filterMessages(List<MessageExt> messageFoundList, String topic, String key, boolean uniqueKeyFlag) {
        List<MessageExt> matchedMessages = new ArrayList<>();
        if (uniqueKeyFlag) {
            matchedMessages.addAll(messageFoundList.stream()
                .filter(msg -> topic.equals(msg.getTopic()))
                .filter(msg -> key.equals(msg.getMsgId()))
                .collect(Collectors.toList())
            );
        } else {
            matchedMessages.addAll(messageFoundList.stream()
                .filter(msg -> topic.equals(msg.getTopic()))
                .filter(msg -> {
                    boolean matched = false;
                    if (StringUtils.isNotBlank(msg.getKeys())) {
                        String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
                        for (String s : keyArray) {
                            if (key.equals(s)) {
                                matched = true;
                                break;
                            }
                        }
                    }
                    return matched;
                }).collect(Collectors.toList()));
        }
        return matchedMessages;
    }

}
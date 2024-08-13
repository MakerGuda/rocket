package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 消费者组信息
 */
@Getter
@Setter
public class ConsumerGroupInfo {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 消费者组名称
     */
    private final String groupName;

    /**
     * 主题和订阅数据 key: topic
     */
    private final ConcurrentMap<String, SubscriptionData> subscriptionTable = new ConcurrentHashMap<>();

    /**
     * key: channel  value: channel info
     */
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = new ConcurrentHashMap<>(16);

    private volatile ConsumeType consumeType;

    private volatile MessageModel messageModel;

    private volatile ConsumeFromWhere consumeFromWhere;

    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }

    public ConsumerGroupInfo(String groupName) {
        this.groupName = groupName;
    }

    public ClientChannelInfo findChannel(final String clientId) {
        for (Entry<Channel, ClientChannelInfo> next : this.channelInfoTable.entrySet()) {
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }
        return null;
    }

    public ClientChannelInfo findChannel(final Channel channel) {
        return this.channelInfoTable.get(channel);
    }

    public List<Channel> getAllChannel() {
        return new ArrayList<>(this.channelInfoTable.keySet());
    }

    /**
     * 获取所有的客户端id
     */
    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();
        for (Entry<Channel, ClientChannelInfo> entry : this.channelInfoTable.entrySet()) {
            ClientChannelInfo clientChannelInfo = entry.getValue();
            result.add(clientChannelInfo.getClientId());
        }
        return result;
    }

    /**
     * 注销channel
     */
    public boolean unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
            return true;
        }
        return false;
    }

    public ClientChannelInfo doChannelCloseEvent(final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn("NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}", info.toString(), groupName);
        }
        return info;
    }

    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        if (null == infoOld) {
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType, messageModel, infoNew.toString());
                updated = true;
            }
            infoOld = infoNew;
        } else {
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error("ConsumerGroupInfo: consumer channel exists in broker, but clientId is not the same one, " + "group={}, old clientChannelInfo={}, new clientChannelInfo={}", groupName, infoOld.toString(), infoNew.toString());
                this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            }
        }
        this.lastUpdateTimestamp = System.currentTimeMillis();
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);
        return updated;
    }

    /**
     * 更新订阅关系
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;
        Set<String> topicSet = new HashSet<>();
        for (SubscriptionData sub : subList) {
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            if (old == null) {
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(sub.getTopic(), sub);
                if (null == prev) {
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}", this.groupName, sub.toString());
                }
            } else if (sub.getSubVersion() > old.getSubVersion()) {
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}", this.groupName, old.toString(), sub.toString());
                }
                this.subscriptionTable.put(sub.getTopic(), sub);
            }
            topicSet.add(sub.getTopic());
        }
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            String oldTopic = next.getKey();
            if (!topicSet.contains(oldTopic)) {
                log.warn("subscription changed, group: {} remove topic {} {}", this.groupName, oldTopic, next.getValue().toString());
                it.remove();
                updated = true;
            }
        }
        this.lastUpdateTimestamp = System.currentTimeMillis();
        return updated;
    }

    /**
     * 获取所有的订阅主题
     */
    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }

    /**
     * 获取主题的订阅信息
     */
    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }

}
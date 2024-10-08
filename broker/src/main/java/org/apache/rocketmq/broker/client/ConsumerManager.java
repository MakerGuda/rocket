package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Getter
@Setter
public class ConsumerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected final BrokerStatsManager brokerStatsManager;

    private final ConcurrentMap<String, ConsumerGroupInfo> consumerTable = new ConcurrentHashMap<>(1024);

    private final ConcurrentMap<String, ConsumerGroupInfo> consumerCompensationTable = new ConcurrentHashMap<>(1024);

    private final List<ConsumerIdsChangeListener> consumerIdsChangeListenerList = new CopyOnWriteArrayList<>();

    private final long channelExpiredTimeout;

    private final long subscriptionExpiredTimeout;

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener, long expiredTimeout) {
        this.consumerIdsChangeListenerList.add(consumerIdsChangeListener);
        this.brokerStatsManager = null;
        this.channelExpiredTimeout = expiredTimeout;
        this.subscriptionExpiredTimeout = expiredTimeout;
    }

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener, final BrokerStatsManager brokerStatsManager, BrokerConfig brokerConfig) {
        this.consumerIdsChangeListenerList.add(consumerIdsChangeListener);
        this.brokerStatsManager = brokerStatsManager;
        this.channelExpiredTimeout = brokerConfig.getChannelExpiredTimeout();
        this.subscriptionExpiredTimeout = brokerConfig.getSubscriptionExpiredTimeout();
    }

    public ClientChannelInfo findChannel(final String group, final String clientId) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(clientId);
        }
        return null;
    }

    public ClientChannelInfo findChannel(final String group, final Channel channel) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(channel);
        }
        return null;
    }

    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        return findSubscriptionData(group, topic, true);
    }

    public SubscriptionData findSubscriptionData(final String group, final String topic, boolean fromCompensationTable) {
        ConsumerGroupInfo consumerGroupInfo = getConsumerGroupInfo(group, false);
        if (consumerGroupInfo != null) {
            SubscriptionData subscriptionData = consumerGroupInfo.findSubscriptionData(topic);
            if (subscriptionData != null) {
                return subscriptionData;
            }
        }
        if (fromCompensationTable) {
            ConsumerGroupInfo consumerGroupCompensationInfo = consumerCompensationTable.get(group);
            if (consumerGroupCompensationInfo != null) {
                return consumerGroupCompensationInfo.findSubscriptionData(topic);
            }
        }
        return null;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return getConsumerGroupInfo(group, false);
    }

    public ConsumerGroupInfo getConsumerGroupInfo(String group, boolean fromCompensationTable) {
        ConsumerGroupInfo consumerGroupInfo = consumerTable.get(group);
        if (consumerGroupInfo == null && fromCompensationTable) {
            consumerGroupInfo = consumerCompensationTable.get(group);
        }
        return consumerGroupInfo;
    }

    public int findSubscriptionDataCount(final String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.getSubscriptionTable().size();
        }
        return 0;
    }

    public boolean doChannelCloseEvent(final Channel channel) {
        boolean removed = false;
        for (Entry<String, ConsumerGroupInfo> next : this.consumerTable.entrySet()) {
            ConsumerGroupInfo info = next.getValue();
            ClientChannelInfo clientChannelInfo = info.doChannelCloseEvent(channel);
            if (clientChannelInfo != null) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, next.getKey(), clientChannelInfo, info.getSubscribeTopics());
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        LOGGER.info("unregister consumer ok, no any connection, and remove consumer group, {}", next.getKey());
                        callConsumerIdsChangeListener(ConsumerGroupEvent.UNREGISTER, next.getKey());
                    }
                }
                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, next.getKey(), info.getAllChannel());
            }
        }
        return removed;
    }

    public void compensateBasicConsumerInfo(String group, ConsumeType consumeType, MessageModel messageModel) {
        ConsumerGroupInfo consumerGroupInfo = consumerCompensationTable.computeIfAbsent(group, ConsumerGroupInfo::new);
        consumerGroupInfo.setConsumeType(consumeType);
        consumerGroupInfo.setMessageModel(messageModel);
    }

    public void compensateSubscribeData(String group, String topic, SubscriptionData subscriptionData) {
        ConsumerGroupInfo consumerGroupInfo = consumerCompensationTable.computeIfAbsent(group, ConsumerGroupInfo::new);
        consumerGroupInfo.getSubscriptionTable().put(topic, subscriptionData);
    }

    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere, final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {
        return registerConsumer(group, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList, isNotifyConsumerIdsChangedEnable, true);
    }

    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere, final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable, boolean updateSubscription) {
        long start = System.currentTimeMillis();
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }
        boolean r1 = consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel, consumeFromWhere);
        if (r1) {
            callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_REGISTER, group, clientChannelInfo, subList.stream().map(SubscriptionData::getTopic).collect(Collectors.toSet()));
        }
        boolean r2 = false;
        if (updateSubscription) {
            r2 = consumerGroupInfo.updateSubscription(subList);
        }
        if (r1 || r2) {
            if (isNotifyConsumerIdsChangedEnable) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
        if (null != this.brokerStatsManager) {
            this.brokerStatsManager.incConsumerRegisterTime((int) (System.currentTimeMillis() - start));
        }
        callConsumerIdsChangeListener(ConsumerGroupEvent.REGISTER, group, subList, clientChannelInfo);
        return r1 || r2;
    }

    public boolean registerConsumerWithoutSub(final String group, final ClientChannelInfo clientChannelInfo, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere, boolean isNotifyConsumerIdsChangedEnable) {
        long start = System.currentTimeMillis();
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }
        boolean updateChannelRst = consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel, consumeFromWhere);
        if (updateChannelRst && isNotifyConsumerIdsChangedEnable) {
            callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
        }
        if (null != this.brokerStatsManager) {
            this.brokerStatsManager.incConsumerRegisterTime((int) (System.currentTimeMillis() - start));
        }
        return updateChannelRst;
    }

    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo, boolean isNotifyConsumerIdsChangedEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            boolean removed = consumerGroupInfo.unregisterChannel(clientChannelInfo);
            if (removed) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo, consumerGroupInfo.getSubscribeTopics());
            }
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                ConsumerGroupInfo remove = this.consumerTable.remove(group);
                if (remove != null) {
                    LOGGER.info("unregister consumer ok, no any connection, and remove consumer group, {}", group);
                    callConsumerIdsChangeListener(ConsumerGroupEvent.UNREGISTER, group);
                }
            }
            if (isNotifyConsumerIdsChangedEnable) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
    }

    public void removeExpireConsumerGroupInfo() {
        List<String> removeList = new ArrayList<>();
        consumerCompensationTable.forEach((group, consumerGroupInfo) -> {
            List<String> removeTopicList = new ArrayList<>();
            ConcurrentMap<String, SubscriptionData> subscriptionTable = consumerGroupInfo.getSubscriptionTable();
            subscriptionTable.forEach((topic, subscriptionData) -> {
                long diff = System.currentTimeMillis() - subscriptionData.getSubVersion();
                if (diff > subscriptionExpiredTimeout) {
                    removeTopicList.add(topic);
                }
            });
            for (String topic : removeTopicList) {
                subscriptionTable.remove(topic);
                if (subscriptionTable.isEmpty()) {
                    removeList.add(group);
                }
            }
        });
        for (String group : removeList) {
            consumerCompensationTable.remove(group);
        }
    }

    public void scanNotActiveChannel() {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            String group = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = consumerGroupInfo.getChannelInfoTable();
            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > channelExpiredTimeout) {
                    LOGGER.warn("SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}", RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo, consumerGroupInfo.getSubscribeTopics());
                    RemotingHelper.closeChannel(clientChannelInfo.getChannel());
                    itChannel.remove();
                }
            }
            if (channelInfoTable.isEmpty()) {
                LOGGER.warn("SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}", group);
                it.remove();
            }
        }
        removeExpireConsumerGroupInfo();
    }

    public HashSet<String> queryTopicConsumeByWho(final String topic) {
        HashSet<String> groups = new HashSet<>();
        for (Entry<String, ConsumerGroupInfo> entry : this.consumerTable.entrySet()) {
            ConcurrentMap<String, SubscriptionData> subscriptionTable = entry.getValue().getSubscriptionTable();
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey());
            }
        }
        return groups;
    }

    public void appendConsumerIdsChangeListener(ConsumerIdsChangeListener listener) {
        consumerIdsChangeListenerList.add(listener);
    }

    protected void callConsumerIdsChangeListener(ConsumerGroupEvent event, String group, Object... args) {
        for (ConsumerIdsChangeListener listener : consumerIdsChangeListenerList) {
            try {
                listener.handle(event, group, args);
            } catch (Throwable t) {
                LOGGER.error("err when call consumerIdsChangeListener", t);
            }
        }
    }

}
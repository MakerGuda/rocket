package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.body.ProducerInfo;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Getter
@Setter
public class ProducerManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;

    private static final int GET_AVAILABLE_CHANNEL_RETRY_COUNT = 3;

    protected final BrokerStatsManager brokerStatsManager;

    /**
     * key: group name
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> groupChannelTable = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Channel> clientChannelTable = new ConcurrentHashMap<>();

    private final List<ProducerChangeListener> producerChangeListenerList = new CopyOnWriteArrayList<>();

    private final PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();

    public ProducerManager() {
        this.brokerStatsManager = null;
    }

    public ProducerManager(final BrokerStatsManager brokerStatsManager) {
        this.brokerStatsManager = brokerStatsManager;
    }

    public boolean groupOnline(String group) {
        Map<Channel, ClientChannelInfo> channels = this.groupChannelTable.get(group);
        return channels != null && !channels.isEmpty();
    }

    public ProducerTableInfo getProducerTable() {
        Map<String, List<ProducerInfo>> map = new HashMap<>();
        for (String group : this.groupChannelTable.keySet()) {
            for (Entry<Channel, ClientChannelInfo> entry : this.groupChannelTable.get(group).entrySet()) {
                ClientChannelInfo clientChannelInfo = entry.getValue();
                if (map.containsKey(group)) {
                    map.get(group).add(new ProducerInfo(clientChannelInfo.getClientId(), clientChannelInfo.getChannel().remoteAddress().toString(), clientChannelInfo.getLanguage(), clientChannelInfo.getVersion(), clientChannelInfo.getLastUpdateTimestamp()));
                } else {
                    map.put(group, new ArrayList<>(Collections.singleton(new ProducerInfo(clientChannelInfo.getClientId(), clientChannelInfo.getChannel().remoteAddress().toString(), clientChannelInfo.getLanguage(), clientChannelInfo.getVersion(), clientChannelInfo.getLastUpdateTimestamp()))));
                }
            }
        }
        return new ProducerTableInfo(map);
    }

    public void scanNotActiveChannel() {
        Iterator<Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>>> iterator = this.groupChannelTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry = iterator.next();
            final String group = entry.getKey();
            final ConcurrentHashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();
            Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Channel, ClientChannelInfo> item = it.next();
                final ClientChannelInfo info = item.getValue();
                long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    it.remove();
                    Channel channelInClientTable = clientChannelTable.get(info.getClientId());
                    if (channelInClientTable != null && channelInClientTable.equals(info.getChannel())) {
                        clientChannelTable.remove(info.getClientId());
                    }
                    log.warn("ProducerManager#scanNotActiveChannel: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}", RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                    callProducerChangeListener(ProducerGroupEvent.CLIENT_UNREGISTER, group, info);
                    RemotingHelper.closeChannel(info.getChannel());
                }
            }
            if (chlMap.isEmpty()) {
                log.warn("SCAN: remove expired channel from ProducerManager groupChannelTable, all clear, group={}", group);
                iterator.remove();
                callProducerChangeListener(ProducerGroupEvent.GROUP_UNREGISTER, group, null);
            }
        }
    }

    public synchronized boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        boolean removed = false;
        if (channel != null) {
            for (final Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable.entrySet()) {
                final String group = entry.getKey();
                final ConcurrentHashMap<Channel, ClientChannelInfo> clientChannelInfoTable = entry.getValue();
                final ClientChannelInfo clientChannelInfo = clientChannelInfoTable.remove(channel);
                if (clientChannelInfo != null) {
                    clientChannelTable.remove(clientChannelInfo.getClientId());
                    removed = true;
                    log.info("NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}", clientChannelInfo.toString(), remoteAddr, group);
                    callProducerChangeListener(ProducerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo);
                    if (clientChannelInfoTable.isEmpty()) {
                        ConcurrentHashMap<Channel, ClientChannelInfo> oldGroupTable = this.groupChannelTable.remove(group);
                        if (oldGroupTable != null) {
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                            callProducerChangeListener(ProducerGroupEvent.GROUP_UNREGISTER, group, null);
                        }
                    }
                }

            }
        }
        return removed;
    }

    public synchronized void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo clientChannelInfoFound;
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.computeIfAbsent(group, k -> new ConcurrentHashMap<>());
        clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
        if (null == clientChannelInfoFound) {
            channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
            log.info("new producer connected, group: {} channel: {}", group, clientChannelInfo.toString());
        }
        if (clientChannelInfoFound != null) {
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    public synchronized void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if (null != channelTable && !channelTable.isEmpty()) {
            ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
            clientChannelTable.remove(clientChannelInfo.getClientId());
            if (old != null) {
                log.info("unregister a producer[{}] from groupChannelTable {}", group, clientChannelInfo.toString());
                callProducerChangeListener(ProducerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo);
            }
            if (channelTable.isEmpty()) {
                this.groupChannelTable.remove(group);
                callProducerChangeListener(ProducerGroupEvent.GROUP_UNREGISTER, group, null);
                log.info("unregister a producer group[{}] from groupChannelTable", group);
            }
        }
    }

    public Channel getAvailableChannel(String groupId) {
        if (groupId == null) {
            return null;
        }
        List<Channel> channelList;
        ConcurrentHashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(groupId);
        if (channelClientChannelInfoHashMap != null) {
            channelList = new ArrayList<>(channelClientChannelInfoHashMap.keySet());
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
            return null;
        }
        int size = channelList.size();
        if (0 == size) {
            log.warn("Channel list is empty. groupId={}", groupId);
            return null;
        }
        Channel lastActiveChannel = null;
        int index = positiveAtomicCounter.incrementAndGet() % size;
        Channel channel = channelList.get(index);
        int count = 0;
        boolean isOk = channel.isActive() && channel.isWritable();
        while (count++ < GET_AVAILABLE_CHANNEL_RETRY_COUNT) {
            if (isOk) {
                return channel;
            }
            if (channel.isActive()) {
                lastActiveChannel = channel;
            }
            index = (++index) % size;
            channel = channelList.get(index);
            isOk = channel.isActive() && channel.isWritable();
        }
        return lastActiveChannel;
    }

    public Channel findChannel(String clientId) {
        return clientChannelTable.get(clientId);
    }

    private void callProducerChangeListener(ProducerGroupEvent event, String group, ClientChannelInfo clientChannelInfo) {
        for (ProducerChangeListener listener : producerChangeListenerList) {
            try {
                listener.handle(event, group, clientChannelInfo);
            } catch (Throwable t) {
                log.error("err when call producerChangeListener", t);
            }
        }
    }

    public void appendProducerChangeListener(ProducerChangeListener producerChangeListener) {
        producerChangeListenerList.add(producerChangeListener);
    }

}
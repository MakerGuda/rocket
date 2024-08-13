package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    private final int cacheSize = 8096;

    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1, ThreadUtils.newGenericThreadFactory("DefaultConsumerIdsChangeListener", true));

    private ConcurrentHashMap<String, List<Channel>> consumerChannelMap = new ConcurrentHashMap<>(cacheSize);

    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;
        scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(brokerController.getBrokerConfig()) {
            @Override
            public void run0() {
                try {
                    notifyConsumerChange();
                } catch (Exception e) {
                    log.error("DefaultConsumerIdsChangeListen#notifyConsumerChange: unexpected error occurs", e);
                }
            }
        }, 30, 15, TimeUnit.SECONDS);
    }

    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if (event == null) {
            return;
        }
        switch (event) {
            case CHANGE:
                if (args == null || args.length < 1) {
                    return;
                }
                List<Channel> channels = (List<Channel>) args[0];
                if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    if (this.brokerController.getBrokerConfig().isRealTimeNotifyConsumerChange()) {
                        for (Channel chl : channels) {
                            this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
                        }
                    } else {
                        consumerChannelMap.put(group, channels);
                    }
                }
                break;
            case UNREGISTER:
                this.brokerController.getConsumerFilterManager().unRegister(group);
                break;
            case REGISTER:
                if (args == null || args.length < 1) {
                    return;
                }
                Collection<SubscriptionData> subscriptionDataList = (Collection<SubscriptionData>) args[0];
                this.brokerController.getConsumerFilterManager().register(group, subscriptionDataList);
                break;
            case CLIENT_REGISTER:
            case CLIENT_UNREGISTER:
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }

    private void notifyConsumerChange() {
        if (consumerChannelMap.isEmpty()) {
            return;
        }
        ConcurrentHashMap<String, List<Channel>> processMap = new ConcurrentHashMap<>(consumerChannelMap);
        consumerChannelMap = new ConcurrentHashMap<>(cacheSize);
        for (Map.Entry<String, List<Channel>> entry : processMap.entrySet()) {
            String consumerId = entry.getKey();
            List<Channel> channelList = entry.getValue();
            try {
                if (channelList != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    for (Channel chl : channelList) {
                        this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, consumerId);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to notify consumer when some consumers changed, consumerId to notify: {}", consumerId, e);
            }
        }
    }

    @Override
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

}
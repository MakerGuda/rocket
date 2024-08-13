package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClientHousekeepingService implements ChannelEventListener {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    private final ScheduledExecutorService scheduledExecutorService;

    public ClientHousekeepingService(final BrokerController brokerController) {
        this.brokerController = brokerController;
        scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1, new ThreadFactoryImpl("ClientHousekeepingScheduledThread", brokerController.getBrokerIdentity()));
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                //扫描异常的channel
                ClientHousekeepingService.this.scanExceptionChannel();
            } catch (Throwable e) {
                log.error("Error occurred when scan not active client channels.", e);
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }

    private void scanExceptionChannel() {
        this.brokerController.getProducerManager().scanNotActiveChannel();
        this.brokerController.getConsumerManager().scanNotActiveChannel();
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
        this.brokerController.getBrokerStatsManager().incChannelConnectNum();
    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(channel);
        this.brokerController.getBrokerStatsManager().incChannelCloseNum();
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(channel);
        this.brokerController.getBrokerStatsManager().incChannelExceptionNum();
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(channel);
        this.brokerController.getBrokerStatsManager().incChannelIdleNum();
    }

    @Override
    public void onChannelActive(String remoteAddr, Channel channel) {

    }

}
package org.apache.rocketmq.client.latency;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo.QueueFilter;
import org.apache.rocketmq.common.message.MessageQueue;

@Getter
@Setter
public class MQFaultStrategy {

    private LatencyFaultTolerance<String> latencyFaultTolerance;

    private volatile boolean sendLatencyFaultEnable;

    private volatile boolean startDetectorEnable;

    private long[] latencyMax = {50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L};

    /**
     * 不可用时长
     */
    private long[] notAvailableDuration = {0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L};
    private ThreadLocal<BrokerFilter> threadBrokerFilter = ThreadLocal.withInitial(BrokerFilter::new);
    /**
     * 判断当前mq所在的broker是否可达
     */
    private QueueFilter reachableFilter = new QueueFilter() {
        @Override
        public boolean filter(MessageQueue mq) {
            return latencyFaultTolerance.isReachable(mq.getBrokerName());
        }
    };
    /**
     * 判断当前mq所在的broker是否可用
     */
    private QueueFilter availableFilter = new QueueFilter() {
        @Override
        public boolean filter(MessageQueue mq) {
            return latencyFaultTolerance.isAvailable(mq.getBrokerName());
        }
    };

    public MQFaultStrategy(ClientConfig cc, Resolver fetcher, ServiceDetector serviceDetector) {
        this.latencyFaultTolerance = new LatencyFaultToleranceImpl(fetcher, serviceDetector);
        this.latencyFaultTolerance.setDetectInterval(cc.getDetectInterval());
        this.latencyFaultTolerance.setDetectTimeout(cc.getDetectTimeout());
        this.setStartDetectorEnable(cc.isStartDetectorEnable());
        this.setSendLatencyFaultEnable(cc.isSendLatencyEnable());
    }

    /**
     * 设置嗅探状态
     */
    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
        this.latencyFaultTolerance.setStartDetectorEnable(startDetectorEnable);
    }

    /**
     * 启动嗅探
     */
    public void startDetector() {
        this.latencyFaultTolerance.startDetector();
    }

    /**
     * 关闭嗅探
     */
    public void shutdown() {
        this.latencyFaultTolerance.shutdown();
    }

    /**
     * 选择一个mq
     *
     * @param tpInfo         主题路由信息
     * @param lastBrokerName 最后一次选择的brokerName
     * @param resetIndex     是否重置索引
     * @return 选中的mq
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName, final boolean resetIndex) {
        //获取当前的broker过滤器
        BrokerFilter brokerFilter = threadBrokerFilter.get();
        brokerFilter.setLastBrokerName(lastBrokerName);
        if (this.sendLatencyFaultEnable) {
            if (resetIndex) {
                tpInfo.resetIndex();
            }
            MessageQueue mq = tpInfo.selectOneMessageQueue(availableFilter, brokerFilter);
            if (mq != null) {
                return mq;
            }
            mq = tpInfo.selectOneMessageQueue(reachableFilter, brokerFilter);
            if (mq != null) {
                return mq;
            }
            return tpInfo.selectOneMessageQueue();
        }
        MessageQueue mq = tpInfo.selectOneMessageQueue(brokerFilter);
        if (mq != null) {
            return mq;
        }
        return tpInfo.selectOneMessageQueue();
    }

    /**
     * 更新broer容错实体
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation, final boolean reachable) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 10000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration, reachable);
        }
    }

    /**
     * 计算不可用时长
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i]) {
                return this.notAvailableDuration[i];
            }
        }
        return 0;
    }

    @Setter
    public static class BrokerFilter implements QueueFilter {

        /**
         * 最后一次发送消息的broker名称
         */
        private String lastBrokerName;

        /**
         * 当前发送的queue所在的broker与上次发送的brokerName不相同时返回true，表示换一个broker发送
         */
        @Override
        public boolean filter(MessageQueue mq) {
            if (lastBrokerName != null) {
                return !mq.getBrokerName().equals(lastBrokerName);
            }
            return true;
        }
    }

}
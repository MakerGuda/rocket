package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class RebalanceService extends ServiceThread {

    private static final long waitInterval = Long.parseLong(System.getProperty("rocketmq.client.rebalance.waitInterval", "20000"));

    private static final long minInterval = Long.parseLong(System.getProperty("rocketmq.client.rebalance.minInterval", "1000"));

    private final Logger log = LoggerFactory.getLogger(RebalanceService.class);

    private final MQClientInstance mqClientFactory;

    private long lastRebalanceTimestamp = System.currentTimeMillis();

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        long realWaitInterval = waitInterval;
        while (!this.isStopped()) {
            this.waitForRunning(realWaitInterval);
            long interval = System.currentTimeMillis() - lastRebalanceTimestamp;
            if (interval < minInterval) {
                realWaitInterval = minInterval - interval;
            } else {
                boolean balanced = this.mqClientFactory.doRebalance();
                realWaitInterval = balanced ? waitInterval : minInterval;
                lastRebalanceTimestamp = System.currentTimeMillis();
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }

}
package org.apache.rocketmq.tools.monitor;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;

import java.util.Map.Entry;
import java.util.TreeMap;

public class DefaultMonitorListener implements MonitorListener {

    private final static String LOG_PREFIX = "[MONITOR] ";

    private final static String LOG_NOTIFY = LOG_PREFIX + " [NOTIFY] ";

    private final Logger logger = LoggerFactory.getLogger(DefaultMonitorListener.class);

    public DefaultMonitorListener() {
    }

    @Override
    public void beginRound() {
        logger.info(LOG_PREFIX + "=========================================beginRound");
    }

    @Override
    public void reportUndoneMsgs(UndoneMsgs undoneMsgs) {
        logger.info(String.format(LOG_PREFIX + "reportUndoneMsgs: %s", undoneMsgs));
    }

    @Override
    public void reportFailedMsgs(FailedMsgs failedMsgs) {
        logger.info(String.format(LOG_PREFIX + "reportFailedMsgs: %s", failedMsgs));
    }

    @Override
    public void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent) {
        logger.info(String.format(LOG_PREFIX + "reportDeleteMsgsEvent: %s", deleteMsgsEvent));
    }

    @Override
    public void reportConsumerRunningInfo(TreeMap<String, ConsumerRunningInfo> criTable) {
        {
            boolean result = ConsumerRunningInfo.analyzeSubscription(criTable);
            if (!result) {
                logger.info(String.format(LOG_NOTIFY + "reportConsumerRunningInfo: ConsumerGroup: %s, Subscription different", criTable.firstEntry().getValue().getProperties().getProperty("consumerGroup")));
            }
        }
        {
            for (Entry<String, ConsumerRunningInfo> next : criTable.entrySet()) {
                String result = ConsumerRunningInfo.analyzeProcessQueue(next.getKey(), next.getValue());
                if (!result.isEmpty()) {
                    logger.info(String.format(LOG_NOTIFY + "reportConsumerRunningInfo: ConsumerGroup: %s, ClientId: %s, %s", criTable.firstEntry().getValue().getProperties().getProperty("consumerGroup"), next.getKey(), result));
                }
            }
        }
    }

    @Override
    public void endRound() {
        logger.info(LOG_PREFIX + "=========================================endRound");
    }

}
package org.apache.rocketmq.broker.longpolling;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

@Getter
@Setter
public class LmqPullRequestHoldService extends PullRequestHoldService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public LmqPullRequestHoldService(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public String getServiceName() {
        if (brokerController != null && brokerController.getBrokerConfig().isInBrokerContainer()) {
            return this.brokerController.getBrokerIdentity().getIdentifier() + LmqPullRequestHoldService.class.getSimpleName();
        }
        return LmqPullRequestHoldService.class.getSimpleName();
    }

    @Override
    public void checkHoldRequest() {
        for (String key : pullRequestTable.keySet()) {
            int idx = key.lastIndexOf(TOPIC_QUEUEID_SEPARATOR);
            if (idx <= 0 || idx >= key.length() - 1) {
                pullRequestTable.remove(key);
                continue;
            }
            String topic = key.substring(0, idx);
            int queueId = Integer.parseInt(key.substring(idx + 1));
            final long offset = brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
            try {
                this.notifyMessageArriving(topic, queueId, offset);
            } catch (Throwable e) {
                LOGGER.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
            }
            if (MixAll.isLmq(topic)) {
                ManyPullRequest mpr = pullRequestTable.get(key);
                if (mpr == null || mpr.getPullRequestList() == null || mpr.getPullRequestList().isEmpty()) {
                    pullRequestTable.remove(key);
                }
            }
        }
    }

}
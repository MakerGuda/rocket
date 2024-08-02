package org.apache.rocketmq.broker.topic;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;

public class RocksDBLmqTopicConfigManager extends RocksDBTopicConfigManager {

    public RocksDBLmqTopicConfigManager(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public TopicConfig selectTopicConfig(final String topic) {
        if (MixAll.isLmq(topic)) {
            return simpleLmqTopicConfig(topic);
        }
        return super.selectTopicConfig(topic);
    }

    @Override
    public void updateTopicConfig(final TopicConfig topicConfig) {
        if (topicConfig == null || MixAll.isLmq(topicConfig.getTopicName())) {
            return;
        }
        super.updateTopicConfig(topicConfig);
    }

    @Override
    public boolean containsTopic(String topic) {
        if (MixAll.isLmq(topic)) {
            return true;
        }
        return super.containsTopic(topic);
    }

    private TopicConfig simpleLmqTopicConfig(String topic) {
        return new TopicConfig(topic, 1, 1, PermName.PERM_READ | PermName.PERM_WRITE);
    }
}

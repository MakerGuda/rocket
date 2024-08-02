package org.apache.rocketmq.client;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.Set;
import java.util.TreeSet;

@Getter
@Setter
public class MQHelper {

    private static final Logger log = LoggerFactory.getLogger(MQHelper.class);

    @Deprecated
    public static void resetOffsetByTimestamp(final MessageModel messageModel, final String consumerGroup, final String topic, final long timestamp) throws Exception {
        resetOffsetByTimestamp(messageModel, "DEFAULT", consumerGroup, topic, timestamp);
    }

    public static void resetOffsetByTimestamp(final MessageModel messageModel, final String instanceName, final String consumerGroup, final String topic, final long timestamp) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
        consumer.setInstanceName(instanceName);
        consumer.setMessageModel(messageModel);
        consumer.start();
        Set<MessageQueue> mqs = null;
        try {
            mqs = consumer.fetchSubscribeMessageQueues(topic);
            if (mqs != null && !mqs.isEmpty()) {
                TreeSet<MessageQueue> mqsNew = new TreeSet<>(mqs);
                for (MessageQueue mq : mqsNew) {
                    long offset = consumer.searchOffset(mq, timestamp);
                    if (offset >= 0) {
                        consumer.updateConsumeOffset(mq, offset);
                        log.info("resetOffsetByTimestamp updateConsumeOffset success, {} {} {}", consumerGroup, offset, mq);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("resetOffsetByTimestamp Exception", e);
            throw e;
        } finally {
            if (mqs != null) {
                consumer.getDefaultMQPullConsumerImpl().getOffsetStore().persistAll(mqs);
            }
            consumer.shutdown();
        }
    }

}
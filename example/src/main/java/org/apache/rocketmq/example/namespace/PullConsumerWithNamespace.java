package org.apache.rocketmq.example.namespace;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PullConsumerWithNamespace {

    public static final String NAMESPACE = "InstanceTest";

    public static final String CONSUMER_GROUP = "cidTest";

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";

    public static final String TOPIC = "NAMESPACE_TOPIC";

    private static final Map<MessageQueue, Long> OFFSET_TABLE = new HashMap<>();

    public static void main(String[] args) throws Exception {
        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(CONSUMER_GROUP);
        pullConsumer.setNamespaceV2(NAMESPACE);
        pullConsumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        pullConsumer.start();
        Set<MessageQueue> mqs = pullConsumer.fetchSubscribeMessageQueues(TOPIC);
        for (MessageQueue mq : mqs) {
            System.out.printf("Consume from the topic: %s, queue: %s%n", mq.getTopic(), mq);
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult = pullConsumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.printf("%s%n", pullResult);
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            dealWithPullResult(pullResult);
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        default:
                            break;
                    }
                } catch (Exception ignore) {
                }
            }
        }
        pullConsumer.shutdown();
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSET_TABLE.get(mq);
        if (offset != null) {
            return offset;
        }
        return 0;
    }

    private static void dealWithPullResult(PullResult pullResult) {
        if (null == pullResult || pullResult.getMsgFoundList().isEmpty()) {
            return;
        }
        pullResult.getMsgFoundList().forEach(msg -> System.out.printf("Topic is:%s, msgId is:%s%n", msg.getTopic(), msg.getMsgId()));
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSET_TABLE.put(mq, offset);
    }

}
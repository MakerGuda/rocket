package org.apache.rocketmq.example.schedule;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

public class ScheduledMessageConsumer {

    public static final String CONSUMER_GROUP = "ExampleConsumer";

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";

    public static final String TOPIC = "TestTopic";

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.subscribe(TOPIC, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                System.out.printf("Receive message[msgId=%s %d  ms later]\n", message.getMsgId(), System.currentTimeMillis() - message.getStoreTimestamp());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
    }

}
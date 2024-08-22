package org.apache.rocketmq.example.namespace;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;

public class PushConsumerWithNamespace {

    public static final String NAMESPACE = "InstanceTest";

    public static final String CONSUMER_GROUP = "cidTest";

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";

    public static final String TOPIC = "NAMESPACE_TOPIC";

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        defaultMQPushConsumer.setNamespaceV2(NAMESPACE);
        defaultMQPushConsumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        defaultMQPushConsumer.subscribe(TOPIC, "*");
        defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> System.out.printf("Msg topic is:%s, MsgId is:%s, reconsumeTimes is:%s%n", msg.getTopic(), msg.getMsgId(), msg.getReconsumeTimes()));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        defaultMQPushConsumer.start();
    }

}
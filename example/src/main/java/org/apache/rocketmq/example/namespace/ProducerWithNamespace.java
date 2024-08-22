package org.apache.rocketmq.example.namespace;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

public class ProducerWithNamespace {

    public static final String NAMESPACE = "InstanceTest";

    public static final String PRODUCER_GROUP = "pidTest";

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";

    public static final int MESSAGE_COUNT = 100;

    public static final String TOPIC = "NAMESPACE_TOPIC";

    public static final String TAG = "tagTest";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamespaceV2(NAMESPACE);
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.start();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message message = new Message(TOPIC, TAG, "Hello world".getBytes(StandardCharsets.UTF_8));
            try {
                SendResult result = producer.send(message);
                System.out.printf("Topic:%s send success, misId is:%s%n", message.getTopic(), result.getMsgId());
            } catch (Exception ignore) {
            }
        }
        producer.shutdown();
    }

}
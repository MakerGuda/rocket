package org.apache.rocketmq.example.schedule;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

public class TimerMessageProducer {

    public static final String PRODUCER_GROUP = "TimerMessageProducerGroup";

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";

    public static final String TOPIC = "TimerTopic";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.start();
        int totalMessagesToSend = 10;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message(TOPIC, ("Hello scheduled message " + i).getBytes(StandardCharsets.UTF_8));
            message.setDeliverTimeMs(System.currentTimeMillis() + 10_000L);
            SendResult result = producer.send(message);
            System.out.printf(result + "\n");
        }
        producer.shutdown();
    }

}
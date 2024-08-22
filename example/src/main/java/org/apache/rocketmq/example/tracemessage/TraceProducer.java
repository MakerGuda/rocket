package org.apache.rocketmq.example.tracemessage;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class TraceProducer {

    public static final String PRODUCER_GROUP = "ProducerGroupName";

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";

    public static final String TOPIC = "TopicTest";

    public static final String TAG = "TagA";

    public static final String KEY = "OrderID188";

    public static final int MESSAGE_COUNT = 128;

    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP, true, null);
        producer.start();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            try {
                {
                    Message msg = new Message(TOPIC, TAG, KEY, "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                }
            } catch (Exception ignore) {
            }
        }
        producer.shutdown();
    }

}
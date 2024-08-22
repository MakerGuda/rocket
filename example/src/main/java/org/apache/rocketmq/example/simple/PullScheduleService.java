package org.apache.rocketmq.example.simple;

import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

public class PullScheduleService {

    public static void main(String[] args) throws MQClientException {
        final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService("GroupName1");
        scheduleService.setMessageModel(MessageModel.CLUSTERING);
        scheduleService.registerPullTaskCallback("TopicTest", (mq, context) -> {
            MQPullConsumer consumer = context.getPullConsumer();
            try {
                long offset = consumer.fetchConsumeOffset(mq, false);
                if (offset < 0) offset = 0;
                PullResult pullResult = consumer.pull(mq, "*", offset, 32);
                System.out.printf("%s%n", offset + "\t" + mq + "\t" + pullResult);
                consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                context.setPullNextDelayTimeMillis(100);
            } catch (Exception ignore) {
            }
        });
        scheduleService.start();
    }

}
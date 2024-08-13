package org.apache.rocketmq.broker.longpolling;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.broker.processor.NotificationProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.store.MessageArrivingListener;

import java.util.Map;

@Getter
@Setter
public class NotifyMessageArrivingListener implements MessageArrivingListener {

    private final PullRequestHoldService pullRequestHoldService;

    private final PopMessageProcessor popMessageProcessor;

    private final NotificationProcessor notificationProcessor;

    public NotifyMessageArrivingListener(final PullRequestHoldService pullRequestHoldService, final PopMessageProcessor popMessageProcessor, final NotificationProcessor notificationProcessor) {
        this.pullRequestHoldService = pullRequestHoldService;
        this.popMessageProcessor = popMessageProcessor;
        this.notificationProcessor = notificationProcessor;
    }

    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        this.pullRequestHoldService.notifyMessageArriving(topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
        this.popMessageProcessor.notifyMessageArriving(topic, queueId, tagsCode, msgStoreTime, filterBitMap, properties);
        this.notificationProcessor.notifyMessageArriving(topic, queueId, tagsCode, msgStoreTime, filterBitMap, properties);
    }

}
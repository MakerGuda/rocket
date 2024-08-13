package org.apache.rocketmq.broker.longpolling;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;

@Getter
@Setter
public class PollingHeader {

    private final String consumerGroup;

    private final String topic;

    private final int queueId;

    private final long bornTime;

    private final long pollTime;

    public PollingHeader(PopMessageRequestHeader requestHeader) {
        this.consumerGroup = requestHeader.getConsumerGroup();
        this.topic = requestHeader.getTopic();
        this.queueId = requestHeader.getQueueId();
        this.bornTime = requestHeader.getBornTime();
        this.pollTime = requestHeader.getPollTime();
    }

    public PollingHeader(NotificationRequestHeader requestHeader) {
        this.consumerGroup = requestHeader.getConsumerGroup();
        this.topic = requestHeader.getTopic();
        this.queueId = requestHeader.getQueueId();
        this.bornTime = requestHeader.getBornTime();
        this.pollTime = requestHeader.getPollTime();
    }

}
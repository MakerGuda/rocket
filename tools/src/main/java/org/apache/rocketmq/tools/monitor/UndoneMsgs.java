package org.apache.rocketmq.tools.monitor;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UndoneMsgs {

    private String consumerGroup;

    private String topic;

    private long undoneMsgsTotal;

    private long undoneMsgsSingleMQ;

    private long undoneMsgsDelayTimeMills;

}
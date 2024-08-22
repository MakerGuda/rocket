package org.apache.rocketmq.tools.monitor;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FailedMsgs {

    private String consumerGroup;

    private String topic;

    private long failedMsgsTotalRecently;

}
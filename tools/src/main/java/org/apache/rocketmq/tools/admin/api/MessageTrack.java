package org.apache.rocketmq.tools.admin.api;

import lombok.Data;

@Data
public class MessageTrack {

    /**
     * 消费者组
     */
    private String consumerGroup;

    /**
     * 跟踪类型
     */
    private TrackType trackType;

    /**
     * 异常描述
     */
    private String exceptionDesc;

}
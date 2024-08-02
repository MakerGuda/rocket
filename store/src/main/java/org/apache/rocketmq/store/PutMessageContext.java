package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PutMessageContext {

    private String topicQueueTableKey;

    private long[] phyPos;

    private int batchSize;

    public PutMessageContext(String topicQueueTableKey) {
        this.topicQueueTableKey = topicQueueTableKey;
    }

}
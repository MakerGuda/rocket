package org.apache.rocketmq.common.consumer;

public enum ConsumeFromWhere {

    /**
     * 从最新的偏移量开始消费
     */
    CONSUME_FROM_LAST_OFFSET,
    /**
     * 从最早的偏移量开始消费
     */
    CONSUME_FROM_FIRST_OFFSET,
    /**
     * 从指定时间戳开始消费
     */
    CONSUME_FROM_TIMESTAMP,

}
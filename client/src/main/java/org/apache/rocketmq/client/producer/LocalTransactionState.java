package org.apache.rocketmq.client.producer;

/**
 * 事务状态枚举
 */
public enum LocalTransactionState {

    /**
     * 提交
     */
    COMMIT_MESSAGE,
    /**
     * 回滚
     */
    ROLLBACK_MESSAGE,
    /**
     * 未知
     */
    UNKNOWN,

}
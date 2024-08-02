package org.apache.rocketmq.client.producer;

public enum SendStatus {
    /**
     * 发送成功
     */
    SEND_OK,
    /**
     * 写磁盘超时
     */
    FLUSH_DISK_TIMEOUT,
    /**
     * 副本同步超时
     */
    FLUSH_SLAVE_TIMEOUT,
    /**
     * 副本不可用
     */
    SLAVE_NOT_AVAILABLE,
}
package org.apache.rocketmq.store.config;

public enum FlushDiskType {
    /**
     * 同步刷盘
     */
    SYNC_FLUSH,
    /**
     * 异步刷盘
     */
    ASYNC_FLUSH
}
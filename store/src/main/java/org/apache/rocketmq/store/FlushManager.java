package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.CompletableFuture;

public interface FlushManager {

    /**
     * 启动
     */
    void start();

    void shutdown();

    void wakeUpFlush();

    void wakeUpCommit();

    CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt);

}
package org.apache.rocketmq.tieredstore.core;

import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.tieredstore.file.FlatFileInterface;

import java.util.concurrent.CompletableFuture;

public interface MessageStoreDispatcher extends CommitLogDispatcher {

    void start();

    void shutdown();

    CompletableFuture<Boolean> doScheduleDispatch(FlatFileInterface flatFile, boolean force);

}
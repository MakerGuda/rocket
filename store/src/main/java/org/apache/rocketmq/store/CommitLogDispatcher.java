package org.apache.rocketmq.store;

import org.rocksdb.RocksDBException;

/**
 * commit log分发器
 */
public interface CommitLogDispatcher {

    /**
     * 从存储系统分发消息，构建消费队列
     */
    void dispatch(final DispatchRequest request) throws RocksDBException;

}
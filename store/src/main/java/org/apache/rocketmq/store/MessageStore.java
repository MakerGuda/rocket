package org.apache.rocketmq.store;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.hook.PutMessageHook;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStoreInterface;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.util.PerfCounter;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * 消息存储接口
 */
public interface MessageStore {

    /**
     * 加载存储的消息数据
     */
    boolean load();

    /**
     * 启动消息存储服务
     */
    void start() throws Exception;

    /**
     * 关闭消息存储服务
     */
    void shutdown();

    /**
     * 销毁消息存储，调用后，所有持久化文件将会被移除
     */
    void destroy();

    /**
     * 异步存储消息
     */
    default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        return CompletableFuture.completedFuture(putMessage(msg));
    }

    /**
     * 异步存储批量消息
     */
    default CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        return CompletableFuture.completedFuture(putMessages(messageExtBatch));
    }

    /**
     * 同步消息存储
     */
    PutMessageResult putMessage(final MessageExtBrokerInner msg);

    /**
     * 同步批量消息存储
     */
    PutMessageResult putMessages(final MessageExtBatch messageExtBatch);

    /**
     * 从指定主题的指定偏移量获取消息
     *
     * @param group         消费者组
     * @param topic         主题
     * @param queueId       消息获取的队列id
     * @param offset        起始偏移量
     * @param maxMsgNums    查询最大消息数
     * @param messageFilter 消息过滤器
     * @return 匹配的消息
     */
    GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final MessageFilter messageFilter);

    /**
     * 异步获取消息
     */
    CompletableFuture<GetMessageResult> getMessageAsync(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final MessageFilter messageFilter);

    /**
     * 同步获取消息
     */
    GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter);

    /**
     * 异步获取消息
     */
    CompletableFuture<GetMessageResult> getMessageAsync(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter);

    /**
     * 获取指定主题，指定队列下的最大偏移量
     */
    long getMaxOffsetInQueue(final String topic, final int queueId);

    /**
     * 返回最大偏移量  true表示获取consumeQueue的最大偏移量  false表示获取commitLog的最大偏移量
     */
    long getMaxOffsetInQueue(final String topic, final int queueId, final boolean committed);

    /**
     * 获取最小偏移量
     */
    long getMinOffsetInQueue(final String topic, final int queueId);

    TimerMessageStore getTimerMessageStore();

    void setTimerMessageStore(TimerMessageStore timerMessageStore);

    /**
     * Get the offset of the message in the commit log, which is also known as physical offset.
     *
     * @param topic              Topic of the message to lookup.
     * @param queueId            Queue ID.
     * @param consumeQueueOffset offset of consume queue.
     * @return physical offset.
     */
    long getCommitLogOffsetInQueue(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * Look up the physical offset of the message whose store timestamp is as specified.
     *
     * @param topic     Topic of the message.
     * @param queueId   Queue ID.
     * @param timestamp Timestamp to look up.
     * @return physical offset which matches.
     */
    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);

    /**
     * Look up the physical offset of the message whose store timestamp is as specified with specific boundaryType.
     *
     * @param topic        Topic of the message.
     * @param queueId      Queue ID.
     * @param timestamp    Timestamp to look up.
     * @param boundaryType Lower or Upper
     * @return physical offset which matches.
     */
    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp, final BoundaryType boundaryType);

    /**
     * Look up the message by given commit log offset.
     *
     * @param commitLogOffset physical offset.
     * @return Message whose physical offset is as specified.
     */
    MessageExt lookMessageByOffset(final long commitLogOffset);

    /**
     * Look up the message by given commit log offset and size.
     *
     * @param commitLogOffset physical offset.
     * @param size            message size
     * @return Message whose physical offset is as specified.
     */
    MessageExt lookMessageByOffset(long commitLogOffset, int size);

    /**
     * Get one message from the specified commit log offset.
     *
     * @param commitLogOffset commit log offset.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset);

    /**
     * Get one message from the specified commit log offset.
     *
     * @param commitLogOffset commit log offset.
     * @param msgSize         message size.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset, final int msgSize);

    /**
     * Get the running information of this store.
     *
     * @return message store running info.
     */
    String getRunningDataInfo();

    long getTimingMessageCount(String topic);

    /**
     * Message store runtime information, which should generally contains various statistical information.
     *
     * @return runtime information of the message store in format of key-value pairs.
     */
    HashMap<String, String> getRuntimeInfo();

    /**
     * HA runtime information
     *
     * @return runtime information of ha
     */
    HARuntimeInfo getHARuntimeInfo();

    /**
     * Get the maximum commit log offset.
     *
     * @return maximum commit log offset.
     */
    long getMaxPhyOffset();

    /**
     * Get the minimum commit log offset.
     *
     * @return minimum commit log offset.
     */
    long getMinPhyOffset();

    /**
     * Get the store time of the earliest message in the given queue.
     *
     * @param topic   Topic of the messages to query.
     * @param queueId Queue ID to find.
     * @return store time of the earliest message.
     */
    long getEarliestMessageTime(final String topic, final int queueId);

    /**
     * Get the store time of the earliest message in this store.
     *
     * @return timestamp of the earliest message in this store.
     */
    long getEarliestMessageTime();

    /**
     * Asynchronous get the store time of the earliest message in this store.
     *
     * @return timestamp of the earliest message in this store.
     * @see #getEarliestMessageTime() getEarliestMessageTime
     */
    CompletableFuture<Long> getEarliestMessageTimeAsync(final String topic, final int queueId);

    /**
     * Get the store time of the message specified.
     *
     * @param topic              message topic.
     * @param queueId            queue ID.
     * @param consumeQueueOffset consume queue offset.
     * @return store timestamp of the message.
     */
    long getMessageStoreTimeStamp(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * Asynchronous get the store time of the message specified.
     *
     * @param topic              message topic.
     * @param queueId            queue ID.
     * @param consumeQueueOffset consume queue offset.
     * @return store timestamp of the message.
     * @see #getMessageStoreTimeStamp(String, int, long) getMessageStoreTimeStamp
     */
    CompletableFuture<Long> getMessageStoreTimeStampAsync(final String topic, final int queueId,
                                                          final long consumeQueueOffset);

    /**
     * Get the total number of the messages in the specified queue.
     *
     * @param topic   Topic
     * @param queueId Queue ID.
     * @return total number.
     */
    long getMessageTotalInQueue(final String topic, final int queueId);

    /**
     * Get the raw commit log data starting from the given offset, which should used for replication purpose.
     *
     * @param offset starting offset.
     * @return commit log data.
     */
    SelectMappedBufferResult getCommitLogData(final long offset);

    /**
     * Get the raw commit log data starting from the given offset, across multiple mapped files.
     *
     * @param offset starting offset.
     * @param size   size of data to get
     * @return commit log data.
     */
    List<SelectMappedBufferResult> getBulkCommitLogData(final long offset, final int size);

    /**
     * Append data to commit log.
     *
     * @param startOffset starting offset.
     * @param data        data to append.
     * @param dataStart   the start index of data array
     * @param dataLength  the length of data array
     * @return true if success; false otherwise.
     */
    boolean appendToCommitLog(final long startOffset, final byte[] data, int dataStart, int dataLength);

    /**
     * Execute file deletion manually.
     */
    void executeDeleteFilesManually();

    /**
     * Query messages by given key.
     *
     * @param topic  topic of the message.
     * @param key    message key.
     * @param maxNum maximum number of the messages possible.
     * @param begin  begin timestamp.
     * @param end    end timestamp.
     */
    QueryMessageResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
                                    final long end);

    /**
     * Asynchronous query messages by given key.
     *
     * @param topic  topic of the message.
     * @param key    message key.
     * @param maxNum maximum number of the messages possible.
     * @param begin  begin timestamp.
     * @param end    end timestamp.
     * @see #queryMessage(String, String, int, long, long) queryMessage
     */
    CompletableFuture<QueryMessageResult> queryMessageAsync(final String topic, final String key, final int maxNum,
                                                            final long begin, final long end);

    /**
     * Update HA master address.
     *
     * @param newAddr new address.
     */
    void updateHaMasterAddress(final String newAddr);

    /**
     * Update master address.
     *
     * @param newAddr new address.
     */
    void updateMasterAddress(final String newAddr);

    /**
     * Return how much the slave falls behind.
     *
     * @return number of bytes that slave falls behind.
     */
    long slaveFallBehindMuch();

    /**
     * Return the current timestamp of the store.
     *
     * @return current time in milliseconds since 1970-01-01.
     */
    long now();

    /**
     * Delete topic's consume queue file and unused stats.
     * This interface allows user delete system topic.
     *
     * @param deleteTopics unused topic name set
     * @return the number of the topics which has been deleted.
     */
    int deleteTopics(final Set<String> deleteTopics);

    /**
     * Clean unused topics which not in retain topic name set.
     *
     * @param retainTopics all valid topics.
     * @return number of the topics deleted.
     */
    int cleanUnusedTopic(final Set<String> retainTopics);

    /**
     * Clean expired consume queues.
     */
    void cleanExpiredConsumerQueue();

    /**
     * Check if the given message has been swapped out of the memory.
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.
     * @return true if the message is no longer in memory; false otherwise.
     * @deprecated As of RIP-57, replaced by {@link #checkInMemByConsumeOffset(String, int, long, int)}, see <a href="https://github.com/apache/rocketmq/issues/5837">this issue</a> for more details
     */
    @Deprecated
    boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    /**
     * Check if the given message is in the page cache.
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.
     * @return true if the message is in page cache; false otherwise.
     */
    boolean checkInMemByConsumeOffset(final String topic, final int queueId, long consumeOffset, int batchSize);

    /**
     * Check if the given message is in store.
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.
     * @return true if the message is in store; false otherwise.
     */
    boolean checkInStoreByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    /**
     * Get number of the bytes that have been stored in commit log and not yet dispatched to consume queue.
     *
     * @return number of the bytes to dispatch.
     */
    long dispatchBehindBytes();

    /**
     * Flush the message store to persist all data.
     *
     * @return maximum offset flushed to persistent storage device.
     */
    long flush();

    /**
     * Get the current flushed offset.
     *
     * @return flushed offset
     */
    long getFlushedWhere();

    /**
     * Reset written offset.
     *
     * @param phyOffset new offset.
     * @return true if success; false otherwise.
     */
    boolean resetWriteOffset(long phyOffset);

    /**
     * Get confirm offset.
     *
     * @return confirm offset.
     */
    long getConfirmOffset();

    /**
     * Set confirm offset.
     *
     * @param phyOffset confirm offset to set.
     */
    void setConfirmOffset(long phyOffset);

    /**
     * 判断操作系统页缓存是否繁忙
     */
    boolean isOSPageCacheBusy();

    /**
     * Get lock time in milliseconds of the store by far.
     *
     * @return lock time in milliseconds.
     */
    long lockTimeMills();

    /**
     * Check if the transient store pool is deficient.
     *
     * @return true if the transient store pool is running out; false otherwise.
     */
    boolean isTransientStorePoolDeficient();

    /**
     * Get the dispatcher list.
     *
     * @return list of the dispatcher.
     */
    LinkedList<CommitLogDispatcher> getDispatcherList();

    /**
     * Add dispatcher.
     *
     * @param dispatcher commit log dispatcher to add
     */
    void addDispatcher(CommitLogDispatcher dispatcher);

    /**
     * Get consume queue of the topic/queue. If consume queue not exist, will return null
     *
     * @param topic   Topic.
     * @param queueId Queue ID.
     * @return Consume queue.
     */
    ConsumeQueueInterface getConsumeQueue(String topic, int queueId);

    /**
     * Get consume queue of the topic/queue. If consume queue not exist, will create one then return it.
     *
     * @param topic   Topic.
     * @param queueId Queue ID.
     * @return Consume queue.
     */
    ConsumeQueueInterface findConsumeQueue(String topic, int queueId);

    /**
     * Get BrokerStatsManager of the messageStore.
     *
     * @return BrokerStatsManager.
     */
    BrokerStatsManager getBrokerStatsManager();

    /**
     * Will be triggered when a new message is appended to commit log.
     *
     * @param msg           the msg that is appended to commit log
     * @param result        append message result
     * @param commitLogFile commit log file
     */
    void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile);

    /**
     * Will be triggered when a new dispatch request is sent to message store.
     *
     * @param dispatchRequest dispatch request
     * @param doDispatch      do dispatch if true
     * @param commitLogFile   commit log file
     * @param isRecover       is from recover process
     * @param isFileEnd       if the dispatch request represents 'file end'
     * @throws RocksDBException only in rocksdb mode
     */
    void onCommitLogDispatch(DispatchRequest dispatchRequest, boolean doDispatch, MappedFile commitLogFile,
                             boolean isRecover, boolean isFileEnd) throws RocksDBException;

    /**
     * Only used in rocksdb mode, because we build consumeQueue in batch(default 16 dispatchRequests)
     * It will be triggered in two cases:
     *
     * @see org.apache.rocketmq.store.DefaultMessageStore.ReputMessageService#doReput
     * @see CommitLog#recoverAbnormally
     */
    void finishCommitLogDispatch();

    /**
     * Get the message store config
     *
     * @return the message store config
     */
    MessageStoreConfig getMessageStoreConfig();

    /**
     * Get the statistics service
     *
     * @return the statistics service
     */
    StoreStatsService getStoreStatsService();

    /**
     * Get the store checkpoint component
     *
     * @return the checkpoint component
     */
    StoreCheckpoint getStoreCheckpoint();

    /**
     * Get the system clock
     *
     * @return the system clock
     */
    SystemClock getSystemClock();

    /**
     * Get the commit log
     *
     * @return the commit log
     */
    CommitLog getCommitLog();

    /**
     * Get running flags
     *
     * @return running flags
     */
    RunningFlags getRunningFlags();

    /**
     * Get the transient store pool
     *
     * @return the transient store pool
     */
    TransientStorePool getTransientStorePool();

    /**
     * Get the HA service
     *
     * @return the HA service
     */
    HAService getHaService();

    /**
     * Get the allocate-mappedFile service
     *
     * @return the allocate-mappedFile service
     */
    AllocateMappedFileService getAllocateMappedFileService();

    /**
     * Truncate dirty logic files
     *
     * @param phyOffset physical offset
     * @throws RocksDBException only in rocksdb mode
     */
    void truncateDirtyLogicFiles(long phyOffset) throws RocksDBException;

    /**
     * Unlock mappedFile
     *
     * @param unlockMappedFile the file that needs to be unlocked
     */
    void unlockMappedFile(MappedFile unlockMappedFile);

    /**
     * Get the perf counter component
     *
     * @return the perf counter component
     */
    PerfCounter.Ticks getPerfCounter();

    /**
     * Get the queue store
     *
     * @return the queue store
     */
    ConsumeQueueStoreInterface getQueueStore();

    /**
     * If 'sync disk flush' is configured in this message store
     *
     * @return yes if true, no if false
     */
    boolean isSyncDiskFlush();

    /**
     * If this message store is sync master role
     *
     * @return yes if true, no if false
     */
    boolean isSyncMaster();

    /**
     * Assign a message to queue offset. If there is a race condition, you need to lock/unlock this method
     * yourself.
     *
     * @param msg message
     * @throws RocksDBException
     */
    void assignOffset(MessageExtBrokerInner msg) throws RocksDBException;

    /**
     * Increase queue offset in memory table. If there is a race condition, you need to lock/unlock this method
     *
     * @param msg        message
     * @param messageNum message num
     */
    void increaseOffset(MessageExtBrokerInner msg, short messageNum);

    /**
     * Get master broker message store in process in broker container
     *
     * @return
     */
    MessageStore getMasterStoreInProcess();

    /**
     * Set master broker message store in process
     *
     * @param masterStoreInProcess
     */
    void setMasterStoreInProcess(MessageStore masterStoreInProcess);

    /**
     * Use FileChannel to get data
     *
     * @param offset
     * @param size
     * @param byteBuffer
     * @return
     */
    boolean getData(long offset, int size, ByteBuffer byteBuffer);

    /**
     * Get the number of alive replicas in group.
     *
     * @return number of alive replicas
     */
    int getAliveReplicaNumInGroup();

    /**
     * Set the number of alive replicas in group.
     *
     * @param aliveReplicaNums number of alive replicas
     */
    void setAliveReplicaNumInGroup(int aliveReplicaNums);

    /**
     * Wake up AutoRecoverHAClient to start HA connection.
     */
    void wakeupHAClient();

    /**
     * Get master flushed offset.
     *
     * @return master flushed offset
     */
    long getMasterFlushedOffset();

    /**
     * Set master flushed offset.
     *
     * @param masterFlushedOffset master flushed offset
     */
    void setMasterFlushedOffset(long masterFlushedOffset);

    /**
     * Get broker init max offset.
     *
     * @return broker max offset in startup
     */
    long getBrokerInitMaxOffset();

    /**
     * Set broker init max offset.
     *
     * @param brokerInitMaxOffset broker init max offset
     */
    void setBrokerInitMaxOffset(long brokerInitMaxOffset);

    /**
     * Calculate the checksum of a certain range of data.
     *
     * @param from begin offset
     * @param to   end offset
     * @return checksum
     */
    byte[] calcDeltaChecksum(long from, long to);

    /**
     * Truncate commitLog and consume queue to certain offset.
     *
     * @param offsetToTruncate offset to truncate
     * @return true if truncate succeed, false otherwise
     * @throws RocksDBException only in rocksdb mode
     */
    boolean truncateFiles(long offsetToTruncate) throws RocksDBException;

    /**
     * Check if the offset is aligned with one message.
     *
     * @param offset offset to check
     * @return true if aligned, false otherwise
     */
    boolean isOffsetAligned(long offset);

    /**
     * Get put message hook list
     *
     * @return List of PutMessageHook
     */
    List<PutMessageHook> getPutMessageHookList();

    /**
     * Get send message back hook
     *
     * @return SendMessageBackHook
     */
    SendMessageBackHook getSendMessageBackHook();

    /**
     * Set send message back hook
     *
     * @param sendMessageBackHook
     */
    void setSendMessageBackHook(SendMessageBackHook sendMessageBackHook);

    //The following interfaces are used for duplication mode

    /**
     * Get last mapped file and return lase file first Offset
     *
     * @return lastMappedFile first Offset
     */
    long getLastFileFromOffset();

    /**
     * Get last mapped file
     *
     * @param startOffset
     * @return true when get the last mapped file, false when get null
     */
    boolean getLastMappedFile(long startOffset);

    /**
     * Set physical offset
     *
     * @param phyOffset
     */
    void setPhysicalOffset(long phyOffset);

    /**
     * Return whether mapped file is empty
     *
     * @return whether mapped file is empty
     */
    boolean isMappedFilesEmpty();

    /**
     * Get state machine version
     *
     * @return state machine version
     */
    long getStateMachineVersion();

    /**
     * Check message and return size
     *
     * @param byteBuffer
     * @param checkCRC
     * @param checkDupInfo
     * @param readBody
     * @return DispatchRequest
     */
    DispatchRequest checkMessageAndReturnSize(final ByteBuffer byteBuffer, final boolean checkCRC,
                                              final boolean checkDupInfo, final boolean readBody);

    /**
     * Get remain transientStoreBuffer numbers
     *
     * @return remain transientStoreBuffer numbers
     */
    int remainTransientStoreBufferNumbs();

    /**
     * Get remain how many data to commit
     *
     * @return remain how many data to commit
     */
    long remainHowManyDataToCommit();

    /**
     * Get remain how many data to flush
     *
     * @return remain how many data to flush
     */
    long remainHowManyDataToFlush();

    /**
     * 判断当前消息存储服务是否已关闭
     */
    boolean isShutdown();

    /**
     * Estimate number of messages, within [from, to], which match given filter
     *
     * @param topic   Topic name
     * @param queueId Queue ID
     * @param from    Lower boundary of the range, inclusive.
     * @param to      Upper boundary of the range, inclusive.
     * @param filter  The message filter.
     * @return Estimate number of messages matching given filter.
     */
    long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter);

    /**
     * Get metrics view of store
     *
     * @return List of metrics selector and view pair
     */
    List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView();

    /**
     * Init store metrics
     *
     * @param meter                     opentelemetry meter
     * @param attributesBuilderSupplier metrics attributes builder
     */
    void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier);

    /**
     * Recover topic queue table
     */
    void recoverTopicQueueTable();

    /**
     * notify message arrive if necessary
     */
    void notifyMessageArriveIfNecessary(DispatchRequest dispatchRequest);
}

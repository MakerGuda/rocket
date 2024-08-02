package org.apache.rocketmq.store.logfile;

import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.FlushDiskType;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

public interface MappedFile {

    /**
     * 返回文件名称
     */
    String getFileName();

    /**
     * 重命名
     */
    void renameTo(String fileName);

    /**
     * 返回mappedFile文件大小
     */
    int getFileSize();

    /**
     * 判断当前mappedFile文件是否已满， 消息无法被追加
     */
    boolean isFull();

    /**
     * 判断当前mappedFile是否可用， 被关闭或销毁时不可用
     */
    boolean isAvailable();

    /**
     * 往mappedFile追加消息
     */
    AppendMessageResult appendMessage(MessageExtBrokerInner message, AppendMessageCallback messageCallback, PutMessageContext putMessageContext);

    /**
     * 往mappedFile追加批量消息
     */
    AppendMessageResult appendMessages(MessageExtBatch message, AppendMessageCallback messageCallback, PutMessageContext putMessageContext);

    /**
     * 追加消息
     */
    AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb);

    /**
     * 追加消息
     */
    boolean appendMessage(byte[] data);

    /**
     * 从指定索引位置追加消息
     */
    boolean appendMessage(byte[] data, int offset, int length);

    /**
     * 返回当前mappedFile全局偏移量， 等于文件名
     */
    long getFileFromOffset();

    /**
     * 消息数据写磁盘
     */
    int flush(int flushLeastPages);

    /**
     * 将二级缓存里的数据写入到页缓存或者磁盘
     */
    int commit(int commitLeastPages);

    /**
     * 从指定开始位置和长度查找byteBuffer
     */
    SelectMappedBufferResult selectMappedBuffer(int pos, int size);

    /**
     * 返回从指定位置开始的byteBuffer
     */
    SelectMappedBufferResult selectMappedBuffer(int pos);

    /**
     * 返回当前mappedFile对应的MappedByteBuffer
     */
    MappedByteBuffer getMappedByteBuffer();

    /**
     * 返回当前mappedFile对应的byteBuffer
     */
    ByteBuffer sliceByteBuffer();

    /**
     * 返回最后一条消息的存储时间戳
     */
    long getStoreTimestamp();

    /**
     * 返回文件的最后修改时间
     */
    long getLastModifiedTimestamp();

    /**
     * 获取指定偏移量指定大小的数据
     */
    boolean getData(int pos, int size, ByteBuffer byteBuffer);

    /**
     * 销毁并从文件系统删除， intervalForcibly 设置为true时，该方法将强制销毁文件，忽略已存在的引用关系
     */
    boolean destroy(long intervalForcibly);

    /**
     * 关闭文件，使文件不可用 intervalForcibly 设置为true时，该方法将强制销毁文件，忽略已存在的引用关系
     */
    void shutdown(long intervalForcibly);

    /**
     * 当引用数为0时，减少文件引用关系并清除文件
     */
    void release();

    /**
     * 增加引用关系
     */
    boolean hold();

    /**
     * 判断当前mappedFile文件是否为消费队列中的第一个文件
     */
    boolean isFirstCreateInQueue();

    /**
     * 当前mappedFile文件如果是消费队列中的第一个，设置为true
     */
    void setFirstCreateInQueue(boolean firstCreateInQueue);

    /**
     * 返回当前mappedFile的刷盘指针
     */
    int getFlushedPosition();

    /**
     * 设置当前mappedFile的刷盘指针
     */
    void setFlushedPosition(int flushedPosition);

    /**
     * 返回当前mappedFile的写指针
     */
    int getWrotePosition();

    /**
     * 设置当前mappedFile的写指针
     */
    void setWrotePosition(int wrotePosition);

    /**
     * 返回当前mappedFile的最大可读指针
     */
    int getReadPosition();

    /**
     * 设置当前mappedFile的提交指针
     */
    void setCommittedPosition(int committedPosition);

    /**
     * 锁定mapped byte buffer
     */
    void mlock();

    /**
     * 解锁mapped byte buffer
     */
    void munlock();

    /**
     * Warm up the mapped bytebuffer
     */
    void warmMappedFile(FlushDiskType type, int pages);

    /**
     * Swap map
     */
    void swapMap();

    /**
     * Clean pageTable
     */
    void cleanSwapedMap(boolean force);

    /**
     * Get recent swap map time
     */
    long getRecentSwapMapTime();

    /**
     * Get recent MappedByteBuffer access count since last swap
     */
    long getMappedByteBufferAccessCountSinceLastSwap();

    /**
     * 获取mappedFile对应的文件
     */
    File getFile();

    /**
     * 重命名文件，添加 .deleted后缀
     */
    void renameToDelete();

    /**
     * 移动当前文件到父目录
     */
    void moveToParent() throws IOException;

    /**
     * 初始化mappedFile
     *
     * @param fileName  文件名称
     * @param fileSize 文件大小
     * @param transientStorePool 堆内存缓冲池
     */
    void init(String fileName, int fileSize, TransientStorePool transientStorePool) throws IOException;

    /**
     * 迭代
     */
    Iterator<SelectMappedBufferResult> iterator(int pos);

    /**
     * 检查指定位置和大小的mapped file是否已加载
     */
    boolean isLoaded(long position, int size);

}
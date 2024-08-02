package org.apache.rocketmq.store.logfile;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.SystemUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Getter
@Setter
public class DefaultMappedFile extends AbstractMappedFile {

    /**
     * 操作系统每页大小，默认4k
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    public static final Unsafe UNSAFE = getUnsafe();

    private static final Method IS_LOADED_METHOD;

    public static final int UNSAFE_PAGE_SIZE = UNSAFE == null ? OS_PAGE_SIZE : UNSAFE.pageSize();

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 当前jvm中mappedFile虚拟内存
     */
    protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 当前jvm中 mappedFile文件个数
     */
    protected static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;

    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;

    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;

    /**
     * 当前文件的写指针，从0开始
     */
    protected volatile int wrotePosition;

    /**
     * 当前文件的提交指针，如果开启transientStorePoolEnable，数据会先存储在transientStorePool中，然后提交到内存映射ByteBuffer中，最后写磁盘
     */
    protected volatile int committedPosition;

    /**
     * 刷盘指针，该指针之前的数据持久到磁盘
     */
    protected volatile int flushedPosition;

    /**
     * 文件大小
     */
    protected int fileSize;

    /**
     * 文件通道
     */
    protected FileChannel fileChannel;

    /**
     * 堆内存ByteBuffer，transientStorePoolEnable为true时不为空
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * 堆内存缓冲池
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * 文件名称
     */
    protected String fileName;

    /**
     * 文件的初始偏移量
     */
    protected long fileFromOffset;

    /**
     * 物理文件
     */
    protected File file;

    /**
     * 物理文件对应的内存映射文件Buffer
     */
    protected MappedByteBuffer mappedByteBuffer;

    /**
     * 文件最后一次写入时间
     */
    protected volatile long storeTimestamp = 0;

    /**
     * 是否为mappedFileQueue中的第一个文件
     */
    protected boolean firstCreateInQueue = false;

    /**
     * 最后一次刷盘时间
     */
    private long lastFlushTime = -1L;

    protected MappedByteBuffer mappedByteBufferWaitToClean = null;

    protected long swapMapTime = 0L;

    protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

    /**
     * 第一条消息的存储时间
     */
    private long startTimestamp = -1;

    /**
     * 最后一条消息的存储时间
     */
    private long stopTimestamp = -1;

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");

        Method isLoaded0method = null;
        if (!SystemUtils.IS_OS_WINDOWS) {
            try {
                isLoaded0method = MappedByteBuffer.class.getDeclaredMethod("isLoaded0", long.class, long.class, int.class);
                isLoaded0method.setAccessible(true);
            } catch (NoSuchMethodException ignore) {
            }
        }
        IS_LOADED_METHOD = isLoaded0method;
    }

    public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public DefaultMappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    @Override
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        UtilAll.ensureDirOK(this.file.getParent());
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    /**
     * 将文件重命名
     */
    @Override
    public void renameTo(String fileName) {
        File newFile = new File(fileName);
        boolean rename = file.renameTo(newFile);
        if (rename) {
            this.fileName = fileName;
            this.file = newFile;
        }
    }

    /**
     * 获取最后一次修改时间
     */
    @Override
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public boolean getData(int pos, int size, ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < size) {
            return false;
        }
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                try {
                    int readNum = fileChannel.read(byteBuffer, pos);
                    return size == readNum;
                } catch (Throwable t) {
                    log.warn("Get data failed pos:{} size:{} fileFromOffset:{}", pos, size, this.fileFromOffset);
                    return false;
                } finally {
                    this.release();
                }
            } else {
                log.debug("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size + ", fileFromOffset: " + this.fileFromOffset);
        }
        return false;
    }

    /**
     * 获取文件大小
     */
    @Override
    public int getFileSize() {
        return fileSize;
    }

    /**
     * 往文件追加消息
     */
    public AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb) {
        assert byteBufferMsg != null;
        assert cb != null;
        //获取当前文件写指针
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = cb.doAppend(byteBuffer, this.fileFromOffset, this.fileSize - currentPos, byteBufferMsg);
            //更新当前mappedFile写指针
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            //更新当前文件最后一次写入时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * 往mappedFile追加消息
     */
    @Override
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb, PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    /**
     * 追加消息
     */
    @Override
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb, PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    /**
     * 往mappedFile追加消息
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb, PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;
        //获取当前mappedFile写指针
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBatch && !((MessageExtBatch) messageExt).isInnerBatch()) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    protected ByteBuffer appendMessageBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return writeBuffer != null ? writeBuffer : this.mappedByteBuffer;
    }

    /**
     * 获取文件的初始偏移量
     */
    @Override
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 往当前mappedFile文件写入消息
     */
    @Override
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    /**
     * 将指定起始位置和长度的部分数据写入到文件
     */
    @Override
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        //获取当前文件的写指针
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            //更新当前文件的写指针
            WROTE_POSITION_UPDATER.addAndGet(this, length);
            return true;
        }
        return false;
    }

    /**
     * 刷盘，将内存中的数据持久化到磁盘
     *
     * @param flushLeastPages 最小刷盘页
     */
    @Override
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();
                try {
                    this.mappedByteBufferAccessCountSinceLastSwap++;
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                    this.lastFlushTime = System.currentTimeMillis();
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                FLUSHED_POSITION_UPDATER.set(this, value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + FLUSHED_POSITION_UPDATER.get(this));
                FLUSHED_POSITION_UPDATER.set(this, getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 提交
     */
    @Override
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            return WROTE_POSITION_UPDATER.get(this);
        }
        if (transientStorePool != null && !transientStorePool.isRealCommit()) {
            COMMITTED_POSITION_UPDATER.set(this, WROTE_POSITION_UPDATER.get(this));
        } else if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + COMMITTED_POSITION_UPDATER.get(this));
            }
        }
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == COMMITTED_POSITION_UPDATER.get(this)) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }
        return COMMITTED_POSITION_UPDATER.get(this);
    }

    /**
     * 执行真正的提交操作
     */
    protected void commit0() {
        int writePos = WROTE_POSITION_UPDATER.get(this);
        int lastCommittedPosition = COMMITTED_POSITION_UPDATER.get(this);
        if (writePos - lastCommittedPosition > 0) {
            try {
                //创建writeBuffer共享缓冲区
                ByteBuffer byteBuffer = writeBuffer.slice();
                //将position回退到上一次的提交位置
                byteBuffer.position(lastCommittedPosition);
                //设置limit为writePosition，即最大有效数据指针
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                //写数据
                this.fileChannel.write(byteBuffer);
                //将提交指针更新为writePosition
                COMMITTED_POSITION_UPDATER.set(this, writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 判断最小刷盘页是否满足刷盘条件
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        //获取当前刷盘指针
        int flush = FLUSHED_POSITION_UPDATER.get(this);
        int write = getReadPosition();
        //当前文件被写满，直接返回true进行刷盘
        if (this.isFull()) {
            return true;
        }
        //判断待刷页数是否满足最小刷盘页数条件
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }
        //刷盘最小页书等于0时，没有限制，只有有未刷的就直接刷
        return write > flush;
    }

    /**
     * 判断是否满足提交条件
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        //获取当前提交指针
        int commit = COMMITTED_POSITION_UPDATER.get(this);
        //获取当前写指针
        int write = WROTE_POSITION_UPDATER.get(this);
        //文件已满返回true
        if (this.isFull()) {
            return true;
        }
        //提交最小页大小大于0时，判断当前写指针与提交指针的差值，除以OS_PAGE_SIZE得到脏页的数量，脏页大于0则提交
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }
        //提交最小页大小等于0时，说明不限制提交页大小，只要写指针大于提交指针，就提交
        return write > commit;
    }

    /**
     * 获取刷盘指针
     */
    @Override
    public int getFlushedPosition() {
        return FLUSHED_POSITION_UPDATER.get(this);
    }

    /**
     * 设置刷盘指针
     */
    @Override
    public void setFlushedPosition(int pos) {
        FLUSHED_POSITION_UPDATER.set(this, pos);
    }

    /**
     * 判断当前文件是否写满，即写指针的位置是否等于文件大小
     */
    @Override
    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    /**
     * 从指定开始位置和长度查找消息
     */
    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        //当前参数没有超过理论消息位置
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size + ", fileFromOffset: " + this.fileFromOffset);
        }
        return null;
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }
        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        //available等于true，表示当前文件可用，无法清理
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have not shutdown, stop unmapping.");
            return false;
        }
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have cleanup, do not do it again.");
            return true;
        }
        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
        this.mappedByteBufferWaitToClean = null;
        //维护类变量，最后返回true表示清理完成
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 文件销毁
     *
     * @param intervalForcibly 拒绝被销毁的最大存活时间
     */
    @Override
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);
        if (this.isCleanupOver()) {
            try {
                long lastModified = getLastModifiedTimestamp();
                //关闭文件通道
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");
                long beginTime = System.currentTimeMillis();
                //物理文件删除
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:" + this.getFlushedPosition() + ", " + UtilAll.computeElapsedTimeMilliseconds(beginTime) + "," + (System.currentTimeMillis() - lastModified));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }
            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName + " Failed. cleanupOver: " + this.cleanupOver);
        }
        return false;
    }

    @Override
    public int getWrotePosition() {
        return WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public void setWrotePosition(int pos) {
        WROTE_POSITION_UPDATER.set(this, pos);
    }

    /**
     * @return 有效数据的最大指针位置
     */
    @Override
    public int getReadPosition() {
        return transientStorePool == null || !transientStorePool.isRealCommit() ? WROTE_POSITION_UPDATER.get(this) : COMMITTED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setCommittedPosition(int pos) {
        COMMITTED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public void warmMappedFile(FlushDiskType type, int pages) {
        this.mappedByteBufferAccessCountSinceLastSwap++;

        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        long flush = 0;
        // long time = System.currentTimeMillis();
        for (long i = 0, j = 0; i < this.fileSize; i += DefaultMappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put((int) i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    @Override
    public void swapMap() {
        if (getRefCount() == 1 && this.mappedByteBufferWaitToClean == null) {

            if (!hold()) {
                log.warn("in swapMap, hold failed, fileName: " + this.fileName);
                return;
            }
            try {
                this.mappedByteBufferWaitToClean = this.mappedByteBuffer;
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
                this.mappedByteBufferAccessCountSinceLastSwap = 0L;
                this.swapMapTime = System.currentTimeMillis();
                log.info("swap file " + this.fileName + " success.");
            } catch (Exception e) {
                log.error("swapMap file " + this.fileName + " Failed. ", e);
            } finally {
                this.release();
            }
        } else {
            log.info("Will not swap file: " + this.fileName + ", ref=" + getRefCount());
        }
    }

    @Override
    public void cleanSwapedMap(boolean force) {
        try {
            if (this.mappedByteBufferWaitToClean == null) {
                return;
            }
            long minGapTime = 120 * 1000L;
            long gapTime = System.currentTimeMillis() - this.swapMapTime;
            if (!force && gapTime < minGapTime) {
                Thread.sleep(minGapTime - gapTime);
            }
            UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
            mappedByteBufferWaitToClean = null;
            log.info("cleanSwapedMap file " + this.fileName + " success.");
        } catch (Exception e) {
            log.error("cleanSwapedMap file " + this.fileName + " Failed. ", e);
        }
    }

    @Override
    public long getRecentSwapMapTime() {
        return 0;
    }

    @Override
    public long getMappedByteBufferAccessCountSinceLastSwap() {
        return this.mappedByteBufferAccessCountSinceLastSwap;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return mappedByteBuffer;
    }

    @Override
    public ByteBuffer sliceByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return this.mappedByteBuffer.slice();
    }

    @Override
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    @Override
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    @Override
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    @Override
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    @Override
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public File getFile() {
        return this.file;
    }

    @Override
    public void renameToDelete() {
        //use Files.move
        if (!fileName.endsWith(".delete")) {
            String newFileName = this.fileName + ".delete";
            try {
                Path newFilePath = Paths.get(newFileName);
                // https://bugs.openjdk.org/browse/JDK-4724038
                // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
                // Windows can't move the file when mmapped.
                if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
                    long position = this.fileChannel.position();
                    UtilAll.cleanBuffer(this.mappedByteBuffer);
                    this.fileChannel.close();
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                    try (RandomAccessFile file = new RandomAccessFile(newFileName, "rw")) {
                        this.fileChannel = file.getChannel();
                        this.fileChannel.position(position);
                        this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                    }
                } else {
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                }
                this.fileName = newFileName;
                this.file = new File(newFileName);
            } catch (IOException e) {
                log.error("move file {} failed", fileName, e);
            }
        }
    }

    @Override
    public void moveToParent() throws IOException {
        Path currentPath = Paths.get(fileName);
        String baseName = currentPath.getFileName().toString();
        Path parentPath = currentPath.getParent().getParent().resolve(baseName);
        // https://bugs.openjdk.org/browse/JDK-4724038
        // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
        // Windows can't move the file when mmapped.
        if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
            long position = this.fileChannel.position();
            UtilAll.cleanBuffer(this.mappedByteBuffer);
            this.fileChannel.close();
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
            try (RandomAccessFile file = new RandomAccessFile(parentPath.toFile(), "rw")) {
                this.fileChannel = file.getChannel();
                this.fileChannel.position(position);
                this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            }
        } else {
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
        }
        this.file = parentPath.toFile();
        this.fileName = parentPath.toString();
    }

    @Override
    public String toString() {
        return this.fileName;
    }

    public Iterator<SelectMappedBufferResult> iterator(int startPos) {
        return new Itr(startPos);
    }

    public static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception ignore) {

        }
        return null;
    }

    public static long mappingAddr(long addr) {
        long offset = addr % UNSAFE_PAGE_SIZE;
        offset = (offset >= 0) ? offset : (UNSAFE_PAGE_SIZE + offset);
        return addr - offset;
    }

    public static int pageCount(long size) {
        return (int) (size + (long) UNSAFE_PAGE_SIZE - 1L) / UNSAFE_PAGE_SIZE;
    }

    @Override
    public boolean isLoaded(long position, int size) {
        if (IS_LOADED_METHOD == null) {
            return true;
        }
        try {
            long addr = ((DirectBuffer) mappedByteBuffer).address() + position;
            return (boolean) IS_LOADED_METHOD.invoke(mappedByteBuffer, mappingAddr(addr), size, pageCount(size));
        } catch (Exception e) {
            log.info("invoke isLoaded0 of file {} error:", file.getAbsolutePath(), e);
        }
        return true;
    }

    private class Itr implements Iterator<SelectMappedBufferResult> {
        private int start;
        private int current;
        private ByteBuffer buf;

        public Itr(int pos) {
            this.start = pos;
            this.current = pos;
            this.buf = mappedByteBuffer.slice();
            this.buf.position(start);
        }

        @Override
        public boolean hasNext() {
            return current < getReadPosition();
        }

        @Override
        public SelectMappedBufferResult next() {
            int readPosition = getReadPosition();
            if (current < readPosition && current >= 0) {
                if (hold()) {
                    ByteBuffer byteBuffer = buf.slice();
                    byteBuffer.position(current);
                    int size = byteBuffer.getInt(current);
                    ByteBuffer bufferResult = byteBuffer.slice();
                    bufferResult.limit(size);
                    current += size;
                    return new SelectMappedBufferResult(fileFromOffset + current, bufferResult, size, DefaultMappedFile.this);
                }
            }
            return null;
        }

        @Override
        public void forEachRemaining(Consumer<? super SelectMappedBufferResult> action) {
            Iterator.super.forEachRemaining(action);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}

package org.apache.rocketmq.store.index;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Index File Header. Format:
 * <pre>
 * ┌───────────────────────────────┬───────────────────────────────┬───────────────────────────────┬───────────────────────────────┬───────────────────┬───────────────────┐
 * │        Begin Timestamp        │          End Timestamp        │     Begin Physical Offset     │       End Physical Offset     │  Hash Slot Count  │    Index Count    │
 * │           (8 Bytes)           │            (8 Bytes)          │           (8 Bytes)           │           (8 Bytes)           │      (4 Bytes)    │      (4 Bytes)    │
 * ├───────────────────────────────┴───────────────────────────────┴───────────────────────────────┴───────────────────────────────┴───────────────────┴───────────────────┤
 * │                                                                      Index File Header                                                                                │
 * │
 * </pre>
 * Index File Header. Size:
 * Begin Timestamp(8) + End Timestamp(8) + Begin Physical Offset(8) + End Physical Offset(8) + Hash Slot Count(4) + Index Count(4) = 40 Bytes
 */
public class IndexHeader {

    public static final int INDEX_HEADER_SIZE = 40;

    private static final int beginTimestampIndex = 0;

    private static final int endTimestampIndex = 8;

    private static final int beginPhyoffsetIndex = 16;

    private static final int endPhyoffsetIndex = 24;

    private static final int hashSlotcountIndex = 32;

    private static final int indexCountIndex = 36;

    private final ByteBuffer byteBuffer;

    /**
     * 索引文件中包含消息的最小存储时间
     */
    private final AtomicLong beginTimestamp = new AtomicLong(0);

    /**
     * 索引文件中包含消息的最大存储时间
     */
    private final AtomicLong endTimestamp = new AtomicLong(0);

    /**
     * 索引文件中包含消息的最小物理偏移量，即commitLog的偏移量
     */
    private final AtomicLong beginPhyOffset = new AtomicLong(0);

    /**
     * 索引文件中包含消息的最大物理偏移量，即commitLog的偏移量
     */
    private final AtomicLong endPhyOffset = new AtomicLong(0);

    /**
     * hashSlot个数
     */
    private final AtomicInteger hashSlotCount = new AtomicInteger(0);

    /**
     * index条目列表中当前已使用个数, index条目在index条目列表中按顺序存储
     */
    private final AtomicInteger indexCount = new AtomicInteger(1);

    public IndexHeader(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public void load() {
        this.beginTimestamp.set(byteBuffer.getLong(beginTimestampIndex));
        this.endTimestamp.set(byteBuffer.getLong(endTimestampIndex));
        this.beginPhyOffset.set(byteBuffer.getLong(beginPhyoffsetIndex));
        this.endPhyOffset.set(byteBuffer.getLong(endPhyoffsetIndex));
        this.hashSlotCount.set(byteBuffer.getInt(hashSlotcountIndex));
        this.indexCount.set(byteBuffer.getInt(indexCountIndex));
        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1);
        }
    }

    public void updateByteBuffer() {
        this.byteBuffer.putLong(beginTimestampIndex, this.beginTimestamp.get());
        this.byteBuffer.putLong(endTimestampIndex, this.endTimestamp.get());
        this.byteBuffer.putLong(beginPhyoffsetIndex, this.beginPhyOffset.get());
        this.byteBuffer.putLong(endPhyoffsetIndex, this.endPhyOffset.get());
        this.byteBuffer.putInt(hashSlotcountIndex, this.hashSlotCount.get());
        this.byteBuffer.putInt(indexCountIndex, this.indexCount.get());
    }

    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
        this.byteBuffer.putLong(beginTimestampIndex, beginTimestamp);
    }

    public long getEndTimestamp() {
        return endTimestamp.get();
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
        this.byteBuffer.putLong(endTimestampIndex, endTimestamp);
    }

    public void setBeginPhyOffset(long beginPhyOffset) {
        this.beginPhyOffset.set(beginPhyOffset);
        this.byteBuffer.putLong(beginPhyoffsetIndex, beginPhyOffset);
    }

    public long getEndPhyOffset() {
        return endPhyOffset.get();
    }

    public void setEndPhyOffset(long endPhyOffset) {
        this.endPhyOffset.set(endPhyOffset);
        this.byteBuffer.putLong(endPhyoffsetIndex, endPhyOffset);
    }

    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
        this.byteBuffer.putInt(hashSlotcountIndex, value);
    }

    public int getIndexCount() {
        return indexCount.get();
    }

    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
        this.byteBuffer.putInt(indexCountIndex, value);
    }

}
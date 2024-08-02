package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
@Setter
public class GetMessageResult {

    private final List<SelectMappedBufferResult> messageMapedList;

    private final List<ByteBuffer> messageBufferList;

    private final List<Long> messageQueueOffset;

    private GetMessageStatus status;

    private long nextBeginOffset;

    private long minOffset;

    private long maxOffset;

    private int bufferTotalSize = 0;

    private int messageCount = 0;

    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;

    private int commercialSizePerMsg = 4 * 1024;

    private long coldDataSum = 0L;

    public static final GetMessageResult NO_MATCH_LOGIC_QUEUE = new GetMessageResult(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE, 0, 0, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    public GetMessageResult() {
        messageMapedList = new ArrayList<>(100);
        messageBufferList = new ArrayList<>(100);
        messageQueueOffset = new ArrayList<>(100);
    }

    public GetMessageResult(int resultSize) {
        messageMapedList = new ArrayList<>(resultSize);
        messageBufferList = new ArrayList<>(resultSize);
        messageQueueOffset = new ArrayList<>(resultSize);
    }

    private GetMessageResult(GetMessageStatus status, long nextBeginOffset, long minOffset, long maxOffset, List<SelectMappedBufferResult> messageMapedList, List<ByteBuffer> messageBufferList, List<Long> messageQueueOffset) {
        this.status = status;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.messageMapedList = messageMapedList;
        this.messageBufferList = messageBufferList;
        this.messageQueueOffset = messageQueueOffset;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(mapedBuffer.getSize() / (double) commercialSizePerMsg);
        this.messageCount++;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(mapedBuffer.getSize() / (double) commercialSizePerMsg);
        this.messageCount++;
        this.messageQueueOffset.add(queueOffset);
    }


    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset, final int batchNum) {
        addMessage(mapedBuffer, queueOffset);
        messageCount += batchNum - 1;
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset=" + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize + ", messageCount=" + messageCount + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }

}
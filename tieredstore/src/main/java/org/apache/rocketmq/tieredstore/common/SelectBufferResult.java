package org.apache.rocketmq.tieredstore.common;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

@Getter
public class SelectBufferResult {

    private final ByteBuffer byteBuffer;

    private final long startOffset;

    private final int size;

    private final long tagCode;

    private final AtomicLong accessCount;

    public SelectBufferResult(ByteBuffer byteBuffer, long startOffset, int size, long tagCode) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.tagCode = tagCode;
        this.accessCount = new AtomicLong();
    }

}
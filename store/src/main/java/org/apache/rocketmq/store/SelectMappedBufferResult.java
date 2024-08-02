package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.store.logfile.MappedFile;

import java.nio.ByteBuffer;

@Getter
@Setter
public class SelectMappedBufferResult {

    private final long startOffset;

    private final ByteBuffer byteBuffer;

    private int size;

    protected MappedFile mappedFile;

    private boolean isInCache = true;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }

    public synchronized void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }

    public boolean isInMem() {
        if (mappedFile == null) {
            return true;
        }
        long pos = startOffset - mappedFile.getFileFromOffset();
        return mappedFile.isLoaded(pos, size);
    }

}
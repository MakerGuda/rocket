package org.apache.rocketmq.store;

import lombok.Data;

@Data
public class RunningFlags {

    private static final int NOT_READABLE_BIT = 1;

    private static final int NOT_WRITEABLE_BIT = 1 << 1;

    private static final int WRITE_LOGICS_QUEUE_ERROR_BIT = 1 << 2;

    private static final int WRITE_INDEX_FILE_ERROR_BIT = 1 << 3;

    private static final int DISK_FULL_BIT = 1 << 4;

    private static final int FENCED_BIT = 1 << 5;

    private static final int LOGIC_DISK_FULL_BIT = 1 << 6;

    private volatile int flagBits = 0;

    public RunningFlags() {
    }

    public boolean isReadable() {
        return (this.flagBits & NOT_READABLE_BIT) == 0;
    }

    public boolean isFenced() {
        return (this.flagBits & FENCED_BIT) != 0;
    }

    public void clearLogicsQueueError() {
        this.flagBits &= ~WRITE_LOGICS_QUEUE_ERROR_BIT;
    }

    public boolean isWriteable() {
        return (this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | DISK_FULL_BIT | WRITE_INDEX_FILE_ERROR_BIT | FENCED_BIT | LOGIC_DISK_FULL_BIT)) == 0;
    }

    public boolean isCQWriteable() {
        return (this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | WRITE_INDEX_FILE_ERROR_BIT | LOGIC_DISK_FULL_BIT)) == 0;
    }

    public void makeLogicsQueueError() {
        this.flagBits |= WRITE_LOGICS_QUEUE_ERROR_BIT;
    }

    public void makeFenced(boolean fenced) {
        if (fenced) {
            this.flagBits |= FENCED_BIT;
        } else {
            this.flagBits &= ~FENCED_BIT;
        }
    }

    public void makeIndexFileError() {
        this.flagBits |= WRITE_INDEX_FILE_ERROR_BIT;
    }

    public boolean getAndMakeDiskFull() {
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        this.flagBits |= DISK_FULL_BIT;
        return result;
    }

    public boolean getAndMakeDiskOK() {
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        this.flagBits &= ~DISK_FULL_BIT;
        return result;
    }

    public boolean getAndMakeLogicDiskFull() {
        boolean result = !((this.flagBits & LOGIC_DISK_FULL_BIT) == LOGIC_DISK_FULL_BIT);
        this.flagBits |= LOGIC_DISK_FULL_BIT;
        return result;
    }

    public boolean getAndMakeLogicDiskOK() {
        boolean result = !((this.flagBits & LOGIC_DISK_FULL_BIT) == LOGIC_DISK_FULL_BIT);
        this.flagBits &= ~LOGIC_DISK_FULL_BIT;
        return result;
    }

}
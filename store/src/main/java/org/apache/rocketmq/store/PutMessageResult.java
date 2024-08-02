package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PutMessageResult {

    private PutMessageStatus putMessageStatus;

    private AppendMessageResult appendMessageResult;

    private boolean remotePut = false;

    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
    }

    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult,
                            boolean remotePut) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
        this.remotePut = remotePut;
    }

    public boolean isOk() {
        if (remotePut) {
            return putMessageStatus == PutMessageStatus.PUT_OK || putMessageStatus == PutMessageStatus.FLUSH_DISK_TIMEOUT || putMessageStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT || putMessageStatus == PutMessageStatus.SLAVE_NOT_AVAILABLE;
        } else {
            return this.appendMessageResult != null && this.appendMessageResult.isOk();
        }
    }

    @Override
    public String toString() {
        return "PutMessageResult [putMessageStatus=" + putMessageStatus + ", appendMessageResult=" + appendMessageResult + ", remotePut=" + remotePut + "]";
    }

}
package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;

import java.util.function.Supplier;

@Getter
@Setter
public class AppendMessageResult {

    /**
     * 返回的状态码
     */
    private AppendMessageStatus status;

    /**
     * 写开始位置的偏移量
     */
    private long wroteOffset;

    /**
     * 写的字节数
     */
    private int wroteBytes;

    /**
     * 消息id
     */
    private String msgId;

    private Supplier<String> msgIdSupplier;

    /**
     * 消息存储时间
     */
    private long storeTimestamp;

    /**
     * 消费队列偏移量
     */
    private long logicsOffset;

    private long pagecacheRT = 0;

    private int msgNum = 1;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, "", 0, 0, 0);
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, String msgId,
                               long storeTimestamp, long logicsOffset, long pagecacheRT) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgId = msgId;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
        this.pagecacheRT = pagecacheRT;
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, long storeTimestamp) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.storeTimestamp = storeTimestamp;
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, Supplier<String> msgIdSupplier, long storeTimestamp, long logicsOffset, long pagecacheRT) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgIdSupplier = msgIdSupplier;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
        this.pagecacheRT = pagecacheRT;
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, Supplier<String> msgIdSupplier, long storeTimestamp, long logicsOffset, long pagecacheRT, int msgNum) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgIdSupplier = msgIdSupplier;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
        this.pagecacheRT = pagecacheRT;
        this.msgNum = msgNum;
    }

    public boolean isOk() {
        return this.status == AppendMessageStatus.PUT_OK;
    }

    public String getMsgId() {
        if (msgId == null && msgIdSupplier != null) {
            msgId = msgIdSupplier.get();
        }
        return msgId;
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" +
                "status=" + status +
                ", wroteOffset=" + wroteOffset +
                ", wroteBytes=" + wroteBytes +
                ", msgId='" + msgId + '\'' +
                ", storeTimestamp=" + storeTimestamp +
                ", logicsOffset=" + logicsOffset +
                ", pagecacheRT=" + pagecacheRT +
                ", msgNum=" + msgNum +
                '}';
    }

}
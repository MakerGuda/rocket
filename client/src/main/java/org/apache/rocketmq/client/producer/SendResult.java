package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息发送的结果
 */
@Getter
@Setter
public class SendResult {

    /**
     * 发送状态
     */
    private SendStatus sendStatus;

    /**
     * 消息id
     */
    private String msgId;

    /**
     * 消息发送到哪一个队列
     */
    private MessageQueue messageQueue;

    /**
     * 消息在队列中的偏移量
     */
    private long queueOffset;

    /**
     * 事务id
     */
    private String transactionId;

    private String offsetMsgId;

    private String regionId;

    private boolean traceOn = true;

    private byte[] rawRespBody;

    public SendResult() {
    }

    public SendResult(SendStatus sendStatus, String msgId, String offsetMsgId, MessageQueue messageQueue, long queueOffset) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.offsetMsgId = offsetMsgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
    }

    public SendResult(final SendStatus sendStatus, final String msgId, final MessageQueue messageQueue,
                      final long queueOffset, final String transactionId,
                      final String offsetMsgId, final String regionId) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
        this.transactionId = transactionId;
        this.offsetMsgId = offsetMsgId;
        this.regionId = regionId;
    }

    @Override
    public String toString() {
        return "SendResult [sendStatus=" + sendStatus + ", msgId=" + msgId + ", offsetMsgId=" + offsetMsgId + ", messageQueue=" + messageQueue + ", queueOffset=" + queueOffset + "]";
    }

}
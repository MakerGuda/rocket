package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;

import java.nio.ByteBuffer;

/**
 * mappedFile写消息后回调函数
 */
public interface AppendMessageCallback {

    /**
     * 消息序列化完成后，写MappedByteBuffer
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final MessageExtBrokerInner msg, PutMessageContext putMessageContext);

    /**
     * 消息序列化完成后，写MappedByteBuffer
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext);

}
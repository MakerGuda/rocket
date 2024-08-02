package org.apache.rocketmq.store;

import java.nio.ByteBuffer;

public interface CompactionAppendMsgCallback {

    AppendMessageResult doAppend(ByteBuffer bbDest, long fileFromOffset, int maxBlank, ByteBuffer bbSrc);

}
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.Map;

public interface MessageFilter {

    /**
     * 根据tag匹配过滤消息
     */
    boolean isMatchedByConsumeQueue(final Long tagsCode, final ConsumeQueueExt.CqExtUnit cqExtUnit);

    boolean isMatchedByCommitLog(final ByteBuffer msgBuffer, final Map<String, String> properties);

}
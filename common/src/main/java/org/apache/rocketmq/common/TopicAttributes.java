package org.apache.rocketmq.common;

import org.apache.rocketmq.common.attribute.Attribute;
import org.apache.rocketmq.common.attribute.EnumAttribute;
import org.apache.rocketmq.common.attribute.LongRangeAttribute;
import org.apache.rocketmq.common.attribute.TopicMessageType;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Sets.newHashSet;

public class TopicAttributes {

    public static final EnumAttribute QUEUE_TYPE_ATTRIBUTE = new EnumAttribute("queue.type", false, newHashSet("BatchCQ", "SimpleCQ"), "SimpleCQ");

    public static final EnumAttribute CLEANUP_POLICY_ATTRIBUTE = new EnumAttribute("cleanup.policy", false, newHashSet("DELETE", "COMPACTION"), "DELETE");

    public static final EnumAttribute TOPIC_MESSAGE_TYPE_ATTRIBUTE = new EnumAttribute("message.type", true, TopicMessageType.topicMessageTypeSet(), TopicMessageType.NORMAL.getValue());

    public static final LongRangeAttribute TOPIC_RESERVE_TIME_ATTRIBUTE = new LongRangeAttribute("reserve.time", true, -1, Long.MAX_VALUE, -1);

    public static final Map<String, Attribute> ALL;

    static {
        ALL = new HashMap<>();
        ALL.put(QUEUE_TYPE_ATTRIBUTE.getName(), QUEUE_TYPE_ATTRIBUTE);
        ALL.put(CLEANUP_POLICY_ATTRIBUTE.getName(), CLEANUP_POLICY_ATTRIBUTE);
        ALL.put(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TOPIC_MESSAGE_TYPE_ATTRIBUTE);
        ALL.put(TOPIC_RESERVE_TIME_ATTRIBUTE.getName(), TOPIC_RESERVE_TIME_ATTRIBUTE);
    }

}
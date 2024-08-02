package org.apache.rocketmq.common.attribute;

import com.google.common.collect.Sets;
import lombok.Getter;
import org.apache.rocketmq.common.message.MessageConst;

import java.util.Map;
import java.util.Set;

@Getter
public enum TopicMessageType {

    UNSPECIFIED("UNSPECIFIED"),

    NORMAL("NORMAL"),

    FIFO("FIFO"),

    DELAY("DELAY"),

    TRANSACTION("TRANSACTION"),

    MIXED("MIXED");

    private final String value;

    TopicMessageType(String value) {
        this.value = value;
    }

    public static Set<String> topicMessageTypeSet() {
        return Sets.newHashSet(UNSPECIFIED.value, NORMAL.value, FIFO.value, DELAY.value, TRANSACTION.value, MIXED.value);
    }

    public static TopicMessageType parseFromMessageProperty(Map<String, String> messageProperty) {
        String isTrans = messageProperty.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        String isTransValue = "true";
        if (isTransValue.equals(isTrans)) {
            return TopicMessageType.TRANSACTION;
        } else if (messageProperty.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null
                || messageProperty.get(MessageConst.PROPERTY_TIMER_DELIVER_MS) != null
                || messageProperty.get(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null
                || messageProperty.get(MessageConst.PROPERTY_TIMER_DELAY_MS) != null) {
            return TopicMessageType.DELAY;
        } else if (messageProperty.get(MessageConst.PROPERTY_SHARDING_KEY) != null) {
            return TopicMessageType.FIFO;
        }
        return TopicMessageType.NORMAL;
    }

    public String getMetricsValue() {
        return value.toLowerCase();
    }

}
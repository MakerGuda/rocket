package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CleanupPolicy;

import java.util.Map;
import java.util.Optional;

public class CleanupPolicyUtils {

    public static CleanupPolicy getDeletePolicy(Optional<TopicConfig> topicConfig) {
        if (!topicConfig.isPresent()) {
            return CleanupPolicy.valueOf(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getDefaultValue());
        }
        String attributeName = TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getName();
        Map<String, String> attributes = topicConfig.get().getAttributes();
        if (attributes == null || attributes.isEmpty()) {
            return CleanupPolicy.valueOf(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getDefaultValue());
        }
        if (attributes.containsKey(attributeName)) {
            return CleanupPolicy.valueOf(attributes.get(attributeName));
        } else {
            return CleanupPolicy.valueOf(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getDefaultValue());
        }
    }

}
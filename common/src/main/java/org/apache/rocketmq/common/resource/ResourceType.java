package org.apache.rocketmq.common.resource;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public enum ResourceType {

    UNKNOWN((byte) 0, "Unknown"),

    ANY((byte) 1, "Any"),

    CLUSTER((byte) 2, "Cluster"),

    NAMESPACE((byte) 3, "Namespace"),

    TOPIC((byte) 4, "Topic"),

    GROUP((byte) 5, "Group");

    @JSONField(value = true)
    private final byte code;

    private final String name;

    ResourceType(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static ResourceType getByName(String name) {
        for (ResourceType resourceType : ResourceType.values()) {
            if (StringUtils.equalsIgnoreCase(resourceType.getName(), name)) {
                return resourceType;
            }
        }
        return null;
    }

}
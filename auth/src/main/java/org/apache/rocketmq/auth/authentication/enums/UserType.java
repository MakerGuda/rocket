package org.apache.rocketmq.auth.authentication.enums;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public enum UserType {

    SUPER((byte) 1, "Super"),

    NORMAL((byte) 2, "Normal");

    @JSONField(value = true)
    private final byte code;

    private final String name;

    UserType(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static UserType getByName(String name) {
        for (UserType subjectType : UserType.values()) {
            if (StringUtils.equalsIgnoreCase(subjectType.getName(), name)) {
                return subjectType;
            }
        }
        return null;
    }

}
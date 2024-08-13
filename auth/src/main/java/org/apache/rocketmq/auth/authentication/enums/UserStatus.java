package org.apache.rocketmq.auth.authentication.enums;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public enum UserStatus {

    ENABLE((byte) 1, "enable"),

    DISABLE((byte) 2, "disable");

    @JSONField(value = true)
    private final byte code;

    private final String name;

    UserStatus(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static UserStatus getByName(String name) {
        for (UserStatus subjectType : UserStatus.values()) {
            if (StringUtils.equalsIgnoreCase(subjectType.getName(), name)) {
                return subjectType;
            }
        }
        return null;
    }

}
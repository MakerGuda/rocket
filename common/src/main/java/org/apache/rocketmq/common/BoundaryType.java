package org.apache.rocketmq.common;

import lombok.Getter;

@Getter
public enum BoundaryType {

    LOWER("lower"),

    UPPER("upper");

    private final String name;

    BoundaryType(String name) {
        this.name = name;
    }

    public static BoundaryType getType(String name) {
        if (BoundaryType.UPPER.getName().equalsIgnoreCase(name)) {
            return UPPER;
        }
        return LOWER;
    }

}
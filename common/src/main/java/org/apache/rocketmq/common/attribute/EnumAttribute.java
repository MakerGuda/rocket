package org.apache.rocketmq.common.attribute;

import lombok.Getter;

import java.util.Set;

@Getter
public class EnumAttribute extends Attribute {

    private final Set<String> universe;

    private final String defaultValue;

    public EnumAttribute(String name, boolean changeable, Set<String> universe, String defaultValue) {
        super(name, changeable);
        this.universe = universe;
        this.defaultValue = defaultValue;
    }

    @Override
    public void verify(String value) {
        if (!this.universe.contains(value)) {
            throw new RuntimeException("value is not in set: " + this.universe);
        }
    }

}
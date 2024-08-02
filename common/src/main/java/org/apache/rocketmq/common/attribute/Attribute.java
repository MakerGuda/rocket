package org.apache.rocketmq.common.attribute;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class Attribute {

    protected String name;

    protected boolean changeable;

    public abstract void verify(String value);

    public Attribute(String name, boolean changeable) {
        this.name = name;
        this.changeable = changeable;
    }

}
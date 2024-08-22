package org.apache.rocketmq.common.attribute;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class Attribute {

    /**
     * 属性名称
     */
    protected String name;

    /**
     * 是否可变
     */
    protected boolean changeable;

    public Attribute(String name, boolean changeable) {
        this.name = name;
        this.changeable = changeable;
    }

    public abstract void verify(String value);

}
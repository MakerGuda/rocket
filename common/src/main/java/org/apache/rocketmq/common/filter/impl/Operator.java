package org.apache.rocketmq.common.filter.impl;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Operator extends Op {

    public static final Operator LEFT_PARENTHESIS = new Operator("(", 30, false);

    public static final Operator RIGHT_PARENTHESIS = new Operator(")", 30, false);

    public static final Operator AND = new Operator("&&", 20, true);

    public static final Operator OR = new Operator("||", 15, true);

    private int priority;

    private boolean comparable;

    private Operator(String symbol, int priority, boolean comparable) {
        super(symbol);
        this.priority = priority;
        this.comparable = comparable;
    }

    public int compare(Operator operator) {
        return Integer.compare(this.priority, operator.priority);
    }

}
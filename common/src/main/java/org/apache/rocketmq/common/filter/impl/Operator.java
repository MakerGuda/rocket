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

    public static Operator createOperator(String operator) {
        if (LEFT_PARENTHESIS.getSymbol().equals(operator)) return LEFT_PARENTHESIS;
        else if (RIGHT_PARENTHESIS.getSymbol().equals(operator)) return RIGHT_PARENTHESIS;
        else if (AND.getSymbol().equals(operator))
            return AND;
        else if (OR.getSymbol().equals(operator))
            return OR;
        else throw new IllegalArgumentException("un support operator " + operator);
    }

    public int compare(Operator operator) {
        return Integer.compare(this.priority, operator.priority);
    }

}
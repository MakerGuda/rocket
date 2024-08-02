package org.apache.rocketmq.common.filter.impl;

import lombok.Getter;

@Getter
public abstract class Op {

    private final String symbol;

    protected Op(String symbol) {
        this.symbol = symbol;
    }

    public String toString() {
        return symbol;
    }

}
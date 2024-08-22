package org.apache.rocketmq.filter.expression;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class BinaryExpression implements Expression {

    protected Expression left;

    protected Expression right;

    public BinaryExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    public int hashCode() {
        return toString().hashCode();
    }

    public boolean equals(Object o) {
        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return toString().equals(o.toString());

    }

    public abstract String getExpressionSymbol();

}
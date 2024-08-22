package org.apache.rocketmq.filter.expression;

public interface BooleanExpression extends Expression {

    boolean matches(EvaluationContext context) throws Exception;

}
package org.apache.rocketmq.filter.expression;

public interface Expression {

    Object evaluate(EvaluationContext context) throws Exception;

}
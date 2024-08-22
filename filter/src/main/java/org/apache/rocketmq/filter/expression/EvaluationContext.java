package org.apache.rocketmq.filter.expression;

import java.util.Map;

public interface EvaluationContext {

    Object get(String name);

    Map<String, Object> keyValues();

}
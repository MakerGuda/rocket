package org.apache.rocketmq.auth.authorization.strategy;

import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;

public interface AuthorizationStrategy {

    void evaluate(AuthorizationContext context);

}
package org.apache.rocketmq.auth.authentication.strategy;

import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;

public interface AuthenticationStrategy {

    void evaluate(AuthenticationContext context);

}
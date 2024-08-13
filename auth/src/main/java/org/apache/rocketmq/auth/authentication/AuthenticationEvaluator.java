package org.apache.rocketmq.auth.authentication;

import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.strategy.AuthenticationStrategy;
import org.apache.rocketmq.auth.config.AuthConfig;

import java.util.function.Supplier;

public class AuthenticationEvaluator {

    private final AuthenticationStrategy authenticationStrategy;

    public AuthenticationEvaluator(AuthConfig authConfig) {
        this(authConfig, null);
    }

    public AuthenticationEvaluator(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authenticationStrategy = AuthenticationFactory.getStrategy(authConfig, metadataService);
    }

    public void evaluate(AuthenticationContext context) {
        if (context == null) {
            return;
        }
        this.authenticationStrategy.evaluate(context);
    }

}
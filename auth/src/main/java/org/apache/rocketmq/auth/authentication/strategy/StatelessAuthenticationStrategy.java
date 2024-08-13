package org.apache.rocketmq.auth.authentication.strategy;

import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.config.AuthConfig;

import java.util.function.Supplier;

public class StatelessAuthenticationStrategy extends AbstractAuthenticationStrategy {

    public StatelessAuthenticationStrategy(AuthConfig authConfig, Supplier<?> metadataService) {
        super(authConfig, metadataService);
    }

    @Override
    public void evaluate(AuthenticationContext context) {
        super.doEvaluate(context);
    }

}
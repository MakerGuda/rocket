package org.apache.rocketmq.auth.authorization.strategy;

import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.config.AuthConfig;

import java.util.function.Supplier;

public class StatelessAuthorizationStrategy extends AbstractAuthorizationStrategy {

    public StatelessAuthorizationStrategy(AuthConfig authConfig, Supplier<?> metadataService) {
        super(authConfig, metadataService);
    }

    @Override
    public void evaluate(AuthorizationContext context) {
        super.doEvaluate(context);
    }

}
package org.apache.rocketmq.auth.authorization;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.strategy.AuthorizationStrategy;
import org.apache.rocketmq.auth.config.AuthConfig;

import java.util.List;
import java.util.function.Supplier;

public class AuthorizationEvaluator {

    private final AuthorizationStrategy authorizationStrategy;

    public AuthorizationEvaluator(AuthConfig authConfig) {
        this(authConfig, null);
    }

    public AuthorizationEvaluator(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authorizationStrategy = AuthorizationFactory.getStrategy(authConfig, metadataService);
    }

    public void evaluate(List<AuthorizationContext> contexts) {
        if (CollectionUtils.isEmpty(contexts)) {
            return;
        }
        contexts.forEach(this.authorizationStrategy::evaluate);
    }

}
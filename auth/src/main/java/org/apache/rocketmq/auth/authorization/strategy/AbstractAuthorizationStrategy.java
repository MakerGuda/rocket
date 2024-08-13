package org.apache.rocketmq.auth.authorization.strategy;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class AbstractAuthorizationStrategy implements AuthorizationStrategy {

    protected final AuthConfig authConfig;

    protected final List<String> authorizationWhitelist = new ArrayList<>();

    protected final AuthorizationProvider<AuthorizationContext> authorizationProvider;

    public AbstractAuthorizationStrategy(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authConfig = authConfig;
        this.authorizationProvider = AuthorizationFactory.getProvider(authConfig);
        if (this.authorizationProvider != null) {
            this.authorizationProvider.initialize(authConfig, metadataService);
        }
        if (StringUtils.isNotBlank(authConfig.getAuthorizationWhitelist())) {
            String[] whitelist = StringUtils.split(authConfig.getAuthorizationWhitelist(), ",");
            for (String rpcCode : whitelist) {
                this.authorizationWhitelist.add(StringUtils.trim(rpcCode));
            }
        }
    }

    public void doEvaluate(AuthorizationContext context) {
        if (context == null) {
            return;
        }
        if (!this.authConfig.isAuthorizationEnabled()) {
            return;
        }
        if (this.authorizationProvider == null) {
            return;
        }
        if (this.authorizationWhitelist.contains(context.getRpcCode())) {
            return;
        }
        try {
            this.authorizationProvider.authorize(context).join();
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Throwable ex) {
            Throwable exception = ExceptionUtils.getRealException(ex);
            if (exception instanceof AuthorizationException) {
                throw (AuthorizationException) exception;
            }
            throw new AuthorizationException("Authorization failed. Please verify your access rights and try again.", exception);
        }
    }

}
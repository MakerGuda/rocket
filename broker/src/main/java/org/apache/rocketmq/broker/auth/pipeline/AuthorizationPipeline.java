package org.apache.rocketmq.broker.auth.pipeline;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.util.List;

public class AuthorizationPipeline implements RequestPipeline {

    protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final AuthConfig authConfig;

    private final AuthorizationEvaluator evaluator;

    public AuthorizationPipeline(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.evaluator = AuthorizationFactory.getEvaluator(authConfig);
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (!authConfig.isAuthorizationEnabled()) {
            return;
        }
        try {
            List<AuthorizationContext> contexts = newContexts(ctx, request);
            evaluator.evaluate(contexts);
        } catch (AuthorizationException | AuthenticationException ex) {
            throw new AbortProcessException(ResponseCode.NO_PERMISSION, ex.getMessage());
        } catch (Throwable ex) {
            LOGGER.error("authorization failed, request:{}", request, ex);
            throw ex;
        }
    }

    protected List<AuthorizationContext> newContexts(ChannelHandlerContext ctx, RemotingCommand request) {
        return AuthorizationFactory.newContexts(authConfig, ctx, request);
    }

}
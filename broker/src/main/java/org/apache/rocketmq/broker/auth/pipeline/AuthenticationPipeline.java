package org.apache.rocketmq.broker.auth.pipeline;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class AuthenticationPipeline implements RequestPipeline {

    protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final AuthConfig authConfig;

    private final AuthenticationEvaluator evaluator;

    public AuthenticationPipeline(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.evaluator = AuthenticationFactory.getEvaluator(authConfig);
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        try {
            AuthenticationContext authenticationContext = newContext(ctx, request);
            evaluator.evaluate(authenticationContext);
        } catch (AuthenticationException ex) {
            throw new AbortProcessException(ResponseCode.NO_PERMISSION, ex.getMessage());
        } catch (Throwable ex) {
            LOGGER.error("authenticate failed, request:{}", request, ex);
            throw ex;
        }
    }

    protected AuthenticationContext newContext(ChannelHandlerContext ctx, RemotingCommand request) {
        return AuthenticationFactory.newContext(authConfig, ctx, request);
    }

}
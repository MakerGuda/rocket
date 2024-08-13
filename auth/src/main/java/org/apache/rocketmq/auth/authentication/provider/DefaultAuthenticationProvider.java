package org.apache.rocketmq.auth.authentication.provider;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.builder.AuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.builder.DefaultAuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.chain.DefaultAuthenticationHandler;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class DefaultAuthenticationProvider implements AuthenticationProvider<DefaultAuthenticationContext> {

    protected final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_AUTH_AUDIT_LOGGER_NAME);

    protected AuthConfig authConfig;

    protected Supplier<?> metadataService;

    protected AuthenticationContextBuilder<DefaultAuthenticationContext> authenticationContextBuilder;

    @Override
    public void initialize(AuthConfig config, Supplier<?> metadataService) {
        this.authConfig = config;
        this.metadataService = metadataService;
        this.authenticationContextBuilder = new DefaultAuthenticationContextBuilder();
    }

    @Override
    public CompletableFuture<Void> authenticate(DefaultAuthenticationContext context) {
        return this.newHandlerChain().handle(context).whenComplete((nil, ex) -> doAuditLog(context, ex));
    }

    @Override
    public DefaultAuthenticationContext newContext(Metadata metadata, GeneratedMessageV3 request) {
        return this.authenticationContextBuilder.build(metadata, request);
    }

    @Override
    public DefaultAuthenticationContext newContext(ChannelHandlerContext context, RemotingCommand command) {
        return this.authenticationContextBuilder.build(context, command);
    }

    protected HandlerChain<DefaultAuthenticationContext, CompletableFuture<Void>> newHandlerChain() {
        return HandlerChain.<DefaultAuthenticationContext, CompletableFuture<Void>>create().addNext(new DefaultAuthenticationHandler(this.authConfig, metadataService));
    }

    protected void doAuditLog(DefaultAuthenticationContext context, Throwable ex) {
        if (StringUtils.isBlank(context.getUsername())) {
            return;
        }
        if (ex != null) {
            log.info("[AUTHENTICATION] User:{} is authenticated failed with Signature = {}.", context.getUsername(), context.getSignature());
        } else {
            log.debug("[AUTHENTICATION] User:{} is authenticated success with Signature = {}.", context.getUsername(), context.getSignature());
        }
    }

}
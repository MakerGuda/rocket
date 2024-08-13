package org.apache.rocketmq.auth.authentication.provider;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface AuthenticationProvider<AuthenticationContext> {

    void initialize(AuthConfig config, Supplier<?> metadataService);

    CompletableFuture<Void> authenticate(AuthenticationContext context);

    AuthenticationContext newContext(Metadata metadata, GeneratedMessageV3 request);

    AuthenticationContext newContext(ChannelHandlerContext context, RemotingCommand command);

}
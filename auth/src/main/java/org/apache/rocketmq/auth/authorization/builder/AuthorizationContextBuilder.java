package org.apache.rocketmq.auth.authorization.builder;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;

public interface AuthorizationContextBuilder {

    List<DefaultAuthorizationContext> build(Metadata metadata, GeneratedMessageV3 message);

    List<DefaultAuthorizationContext> build(ChannelHandlerContext context, RemotingCommand command);

}
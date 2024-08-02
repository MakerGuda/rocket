package org.apache.rocketmq.client.impl.mqclient;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DoNothingClientRemotingProcessor extends ClientRemotingProcessor {

    public DoNothingClientRemotingProcessor(MQClientInstance mqClientFactory) {
        super(mqClientFactory);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        return null;
    }

}
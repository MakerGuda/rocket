package org.apache.rocketmq.broker.longpolling;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.MessageFilter;

@Getter
@Setter
public class PullRequest {

    private final RemotingCommand requestCommand;

    private final Channel clientChannel;

    private final long timeoutMillis;

    private final long suspendTimestamp;

    private final long pullFromThisOffset;

    private final SubscriptionData subscriptionData;

    private final MessageFilter messageFilter;

    public PullRequest(RemotingCommand requestCommand, Channel clientChannel, long timeoutMillis, long suspendTimestamp, long pullFromThisOffset, SubscriptionData subscriptionData, MessageFilter messageFilter) {
        this.requestCommand = requestCommand;
        this.clientChannel = clientChannel;
        this.timeoutMillis = timeoutMillis;
        this.suspendTimestamp = suspendTimestamp;
        this.pullFromThisOffset = pullFromThisOffset;
        this.subscriptionData = subscriptionData;
        this.messageFilter = messageFilter;
    }

}
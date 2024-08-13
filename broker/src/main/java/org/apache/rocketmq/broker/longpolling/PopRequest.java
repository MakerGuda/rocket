package org.apache.rocketmq.broker.longpolling;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.MessageFilter;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Setter
public class PopRequest {

    public static final Comparator<PopRequest> COMPARATOR = (o1, o2) -> {
        int ret = (int) (o1.getExpired() - o2.getExpired());
        if (ret != 0) {
            return ret;
        }
        ret = (int) (o1.op - o2.op);
        if (ret != 0) {
            return ret;
        }
        return -1;
    };

    private static final AtomicLong COUNTER = new AtomicLong(Long.MIN_VALUE);

    private final RemotingCommand remotingCommand;

    private final ChannelHandlerContext ctx;

    private final AtomicBoolean complete = new AtomicBoolean(false);

    private final long op = COUNTER.getAndIncrement();

    private final long expired;

    private final SubscriptionData subscriptionData;

    private final MessageFilter messageFilter;

    public PopRequest(RemotingCommand remotingCommand, ChannelHandlerContext ctx, long expired, SubscriptionData subscriptionData, MessageFilter messageFilter) {
        this.ctx = ctx;
        this.remotingCommand = remotingCommand;
        this.expired = expired;
        this.subscriptionData = subscriptionData;
        this.messageFilter = messageFilter;
    }

    public Channel getChannel() {
        return ctx.channel();
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() > (expired - 50);
    }

    public boolean complete() {
        return complete.compareAndSet(false, true);
    }

}
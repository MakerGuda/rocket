package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface InvokeCallback {

    void operationComplete(final ResponseFuture responseFuture);

    default void operationSucceed(final RemotingCommand response) {

    }

    default void operationFail(final Throwable throwable) {

    }

}
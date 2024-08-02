package org.apache.rocketmq.common;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Set;

public interface LockCallback {

    void onSuccess(final Set<MessageQueue> lockOKMQSet);

    void onException(final Throwable e);

}
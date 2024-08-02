package org.apache.rocketmq.common;

public interface UnlockCallback {

    void onSuccess();

    void onException(final Throwable e);

}
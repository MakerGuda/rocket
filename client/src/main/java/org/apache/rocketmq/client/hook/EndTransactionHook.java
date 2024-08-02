package org.apache.rocketmq.client.hook;

public interface EndTransactionHook {

    String hookName();

    void endTransaction(final EndTransactionContext context);

}
package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public interface TransactionListener {

    /**
     * 发送事务半消息成功时，该方法将会被调用执行本地事务
     *
     * @param msg 半消息
     * @param arg 自定义业务参数
     * @return 本地事务状态
     */
    LocalTransactionState executeLocalTransaction(final Message msg, final Object arg);

    /**
     * 发送事务半消息没有响应时，broker将会发送检查消息，检查事务状态，此方法将会返回本地事务状态
     *
     * @param msg 检查消息
     * @return 本地事务状态
     */
    LocalTransactionState checkLocalTransaction(final MessageExt msg);

}
package org.apache.rocketmq.store.hook;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.PutMessageResult;

public interface PutMessageHook {

    /**
     * hook名称
     */
    String hookName();

    /**
     * 存放消息之前执行，例如消息验证，检查等
     */
    PutMessageResult executeBeforePutMessage(MessageExt msg);

}
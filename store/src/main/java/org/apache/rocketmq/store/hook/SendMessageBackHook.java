package org.apache.rocketmq.store.hook;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public interface SendMessageBackHook {

    /**
     * Slave send message back to master at certain offset when HA handshake
     */
    boolean executeSendMessageBack(List<MessageExt> msgList, String brokerName, String brokerAddr);

}
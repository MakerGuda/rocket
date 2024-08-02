package org.apache.rocketmq.client.hook;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.common.message.Message;

@Getter
@Setter
public class EndTransactionContext {

    private String producerGroup;

    private Message message;

    private String brokerAddr;

    private String msgId;

    private String transactionId;

    private LocalTransactionState transactionState;

    private boolean fromTransactionCheck;

}
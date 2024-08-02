package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransactionSendResult extends SendResult {

    private LocalTransactionState localTransactionState;

    public TransactionSendResult() {
    }

}
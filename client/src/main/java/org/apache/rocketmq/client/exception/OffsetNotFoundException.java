package org.apache.rocketmq.client.exception;

public class OffsetNotFoundException extends MQBrokerException {

    public OffsetNotFoundException(int responseCode, String errorMessage, String brokerAddr) {
        super(responseCode, errorMessage, brokerAddr);
    }

}
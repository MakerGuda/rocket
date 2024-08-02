package org.apache.rocketmq.client.exception;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;

@Getter
@Setter
public class MQBrokerException extends Exception {

    private static final long serialVersionUID = 5975020272601250368L;

    private final int responseCode;

    private final String errorMessage;

    private final String brokerAddr;

    public MQBrokerException(int responseCode, String errorMessage) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
        this.brokerAddr = null;
    }

    public MQBrokerException(int responseCode, String errorMessage, String brokerAddr) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage + (brokerAddr != null ? " BROKER: " + brokerAddr : "")));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
        this.brokerAddr = brokerAddr;
    }

}
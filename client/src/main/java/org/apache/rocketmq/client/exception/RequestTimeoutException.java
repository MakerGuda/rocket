package org.apache.rocketmq.client.exception;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;

@Getter
@Setter
public class RequestTimeoutException extends Exception {

    private static final long serialVersionUID = -5758410930844185841L;

    private int responseCode;

    private String errorMessage;

    public RequestTimeoutException(int responseCode, String errorMessage) {
        super("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

}
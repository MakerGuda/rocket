package org.apache.rocketmq.filter.expression;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MQFilterException extends Exception {

    private static final long serialVersionUID = 1L;

    private final int responseCode;

    private final String errorMessage;

    public MQFilterException(String errorMessage, Throwable cause) {
        super(cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public MQFilterException(int responseCode, String errorMessage) {
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

}
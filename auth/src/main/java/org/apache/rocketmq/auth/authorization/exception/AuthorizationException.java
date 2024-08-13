package org.apache.rocketmq.auth.authorization.exception;

import org.slf4j.helpers.MessageFormatter;

public class AuthorizationException extends RuntimeException {

    public AuthorizationException(String message) {
        super(message);
    }

    public AuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuthorizationException(String messagePattern, Object... argArray) {
        super(MessageFormatter.arrayFormat(messagePattern, argArray).getMessage());
    }

}
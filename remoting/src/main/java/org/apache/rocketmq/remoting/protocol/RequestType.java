package org.apache.rocketmq.remoting.protocol;

import lombok.Getter;

@Getter
public enum RequestType {

    STREAM((byte) 0);

    private final byte code;

    RequestType(byte code) {
        this.code = code;
    }

    public static RequestType valueOf(byte code) {
        for (RequestType requestType : RequestType.values()) {
            if (requestType.getCode() == code) {
                return requestType;
            }
        }
        return null;
    }

}
package org.apache.rocketmq.common.message;

import lombok.Getter;
import lombok.Setter;

import java.net.SocketAddress;

@Getter
@Setter
public class MessageId {

    private SocketAddress address;

    private long offset;

    public MessageId(SocketAddress address, long offset) {
        this.address = address;
        this.offset = offset;
    }

}
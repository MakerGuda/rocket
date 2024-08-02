package org.apache.rocketmq.remoting.protocol.header.namesrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetKVConfigResponseHeader implements CommandCustomHeader {

    /**
     * 值
     */
    @CFNullable
    private String value;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
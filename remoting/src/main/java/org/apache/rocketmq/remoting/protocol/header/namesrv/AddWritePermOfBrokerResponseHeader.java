package org.apache.rocketmq.remoting.protocol.header.namesrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class AddWritePermOfBrokerResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private Integer addTopicCount;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Integer getAddTopicCount() {
        return addTopicCount;
    }

    public void setAddTopicCount(Integer addTopicCount) {
        this.addTopicCount = addTopicCount;
    }

}
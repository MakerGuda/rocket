package org.apache.rocketmq.remoting.protocol.heartbeat;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ProducerData {

    private String groupName;

    @Override
    public String toString() {
        return "ProducerData [groupName=" + groupName + "]";
    }

}
package org.apache.rocketmq.remoting.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class GetConsumerStatusBody extends RemotingSerializable {

    private Map<MessageQueue, Long> messageQueueTable = new HashMap<>();

    private Map<String, Map<MessageQueue, Long>> consumerTable = new HashMap<>();

}
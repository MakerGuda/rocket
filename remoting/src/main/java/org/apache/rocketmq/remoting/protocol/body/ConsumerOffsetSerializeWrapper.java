package org.apache.rocketmq.remoting.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Getter
@Setter
public class ConsumerOffsetSerializeWrapper extends RemotingSerializable {

    /**
     * key: topicGroup
     */
    private ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>(512);

    private DataVersion dataVersion;

}
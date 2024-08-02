package org.apache.rocketmq.broker.offset;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Getter
@Setter
public class RocksDBOffsetSerializeWrapper extends RemotingSerializable {

    private ConcurrentMap<Integer, Long> offsetTable = new ConcurrentHashMap<>(16);

}
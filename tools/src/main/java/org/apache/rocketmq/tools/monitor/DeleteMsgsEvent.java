package org.apache.rocketmq.tools.monitor;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.topic.OffsetMovedEvent;

@Getter
@Setter
public class DeleteMsgsEvent {

    private OffsetMovedEvent offsetMovedEvent;

    private long eventTimestamp;

}
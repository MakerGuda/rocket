package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;

@Getter
@Setter
@RocketMQAction(value = RequestCode.GET_MAX_OFFSET, action = Action.GET)
public class GetMaxOffsetRequestHeader extends TopicQueueRequestHeader {

    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
    private String topic;

    @CFNotNull
    private Integer queueId;

    /**
     * A message at committed offset has been dispatched from Topic to MessageQueue, so it can be consumed immediately,
     * while a message at inflight offset is not visible for a consumer temporarily.
     * Set this flag true if the max committed offset is needed, or false if the max inflight offset is preferred.
     * The default value is true.
     */
    @CFNullable
    private boolean committed = true;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("topic", topic)
            .add("queueId", queueId)
            .add("committed", committed)
            .toString();
    }

}
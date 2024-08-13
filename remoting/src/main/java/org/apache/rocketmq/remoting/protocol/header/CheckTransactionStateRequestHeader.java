package org.apache.rocketmq.remoting.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.RpcRequestHeader;

@Getter
@Setter
@RocketMQAction(value = RequestCode.CHECK_TRANSACTION_STATE, action = Action.PUB)
public class CheckTransactionStateRequestHeader extends RpcRequestHeader {

    @RocketMQResource(ResourceType.TOPIC)
    private String topic;

    @CFNotNull
    private Long tranStateTableOffset;

    @CFNotNull
    private Long commitLogOffset;

    private String msgId;

    private String transactionId;

    private String offsetMsgId;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

}
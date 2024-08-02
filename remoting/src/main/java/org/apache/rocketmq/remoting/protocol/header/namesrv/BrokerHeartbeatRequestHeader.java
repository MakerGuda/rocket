package org.apache.rocketmq.remoting.protocol.header.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@Getter
@Setter
@RocketMQAction(value = RequestCode.BROKER_HEARTBEAT, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class BrokerHeartbeatRequestHeader implements CommandCustomHeader {

    /**
     * 集群名称
     */
    @CFNotNull
    @RocketMQResource(ResourceType.CLUSTER)
    private String clusterName;

    /**
     * broker地址
     */
    @CFNotNull
    private String brokerAddr;

    /**
     * broker名称
     */
    @CFNotNull
    private String brokerName;

    /**
     * brokerId
     */
    @CFNullable
    private Long brokerId;

    @CFNullable
    private Integer epoch;

    @CFNullable
    private Long maxOffset;

    @CFNullable
    private Long confirmOffset;

    /**
     * 心跳检测超时时间
     */
    @CFNullable
    private Long heartbeatTimeoutMills;

    @CFNullable
    private Integer electionPriority;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

}
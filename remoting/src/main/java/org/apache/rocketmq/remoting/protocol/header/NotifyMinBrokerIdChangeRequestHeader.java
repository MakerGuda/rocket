package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class NotifyMinBrokerIdChangeRequestHeader implements CommandCustomHeader {

    /**
     * 最小brokerId
     */
    @CFNullable
    private Long minBrokerId;

    /**
     * broker名称
     */
    @CFNullable
    private String brokerName;

    /**
     * 最小broker地址
     */
    @CFNullable
    private String minBrokerAddr;

    /**
     * 下线broker地址
     */
    @CFNullable
    private String offlineBrokerAddr;

    @CFNullable
    private String haBrokerAddr;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public Long getMinBrokerId() {
        return minBrokerId;
    }

    public void setMinBrokerId(Long minBrokerId) {
        this.minBrokerId = minBrokerId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getMinBrokerAddr() {
        return minBrokerAddr;
    }

    public void setMinBrokerAddr(String minBrokerAddr) {
        this.minBrokerAddr = minBrokerAddr;
    }

    public String getOfflineBrokerAddr() {
        return offlineBrokerAddr;
    }

    public void setOfflineBrokerAddr(String offlineBrokerAddr) {
        this.offlineBrokerAddr = offlineBrokerAddr;
    }

    public String getHaBrokerAddr() {
        return haBrokerAddr;
    }

    public void setHaBrokerAddr(String haBrokerAddr) {
        this.haBrokerAddr = haBrokerAddr;
    }

}
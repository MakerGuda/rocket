package org.apache.rocketmq.remoting.protocol.header.namesrv;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.DELETE_KV_CONFIG, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class DeleteKVConfigRequestHeader implements CommandCustomHeader {

    /**
     * 命名空间
     */
    @CFNotNull
    private String namespace;

    /**
     * 键
     */
    @CFNotNull
    private String key;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}
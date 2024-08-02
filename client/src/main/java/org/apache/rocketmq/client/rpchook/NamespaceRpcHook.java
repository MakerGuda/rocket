package org.apache.rocketmq.client.rpchook;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class NamespaceRpcHook implements RPCHook {

    private final ClientConfig clientConfig;

    public NamespaceRpcHook(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        if (StringUtils.isNotEmpty(clientConfig.getNamespaceV2())) {
            request.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD, "true");
            request.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD, clientConfig.getNamespaceV2());
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }

}
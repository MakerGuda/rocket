package org.apache.rocketmq.broker.loadbalance;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;

import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class MessageRequestModeManager extends ConfigManager {

    private transient BrokerController brokerController;

    /**
     * key : topic value: {key: consumerGroup}
     */
    private ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> messageRequestModeMap = new ConcurrentHashMap<>();

    public MessageRequestModeManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void setMessageRequestMode(String topic, String consumerGroup, SetMessageRequestModeRequestBody requestBody) {
        ConcurrentHashMap<String, SetMessageRequestModeRequestBody> consumerGroup2ModeMap = messageRequestModeMap.get(topic);
        if (consumerGroup2ModeMap == null) {
            consumerGroup2ModeMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, SetMessageRequestModeRequestBody> pre = messageRequestModeMap.putIfAbsent(topic, consumerGroup2ModeMap);
            if (pre != null) {
                consumerGroup2ModeMap = pre;
            }
        }
        consumerGroup2ModeMap.put(consumerGroup, requestBody);
    }

    public SetMessageRequestModeRequestBody getMessageRequestMode(String topic, String consumerGroup) {
        ConcurrentHashMap<String, SetMessageRequestModeRequestBody> consumerGroup2ModeMap = messageRequestModeMap.get(topic);
        if (consumerGroup2ModeMap != null) {
            return consumerGroup2ModeMap.get(consumerGroup);
        }
        return null;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getMessageRequestModePath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            MessageRequestModeManager obj = RemotingSerializable.fromJson(jsonString, MessageRequestModeManager.class);
            if (obj != null) {
                this.messageRequestModeMap = obj.messageRequestModeMap;
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

}
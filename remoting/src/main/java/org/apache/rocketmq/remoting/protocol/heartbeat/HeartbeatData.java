package org.apache.rocketmq.remoting.protocol.heartbeat;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
public class HeartbeatData extends RemotingSerializable {

    private String clientID;

    private Set<ProducerData> producerDataSet = new HashSet<>();

    private Set<ConsumerData> consumerDataSet = new HashSet<>();

    private int heartbeatFingerprint = 0;

    private boolean isWithoutSub = false;

    @Override
    public String toString() {
        return "HeartbeatData [clientID=" + clientID + ", producerDataSet=" + producerDataSet + ", consumerDataSet=" + consumerDataSet + "]";
    }

    public int computeHeartbeatFingerprint() {
        HeartbeatData heartbeatDataCopy = JSON.parseObject(JSON.toJSONString(this), HeartbeatData.class);
        for (ConsumerData consumerData : heartbeatDataCopy.getConsumerDataSet()) {
            for (SubscriptionData subscriptionData : consumerData.getSubscriptionDataSet()) {
                subscriptionData.setSubVersion(0L);
            }
        }
        heartbeatDataCopy.setWithoutSub(false);
        heartbeatDataCopy.setHeartbeatFingerprint(0);
        heartbeatDataCopy.setClientID("");
        return JSON.toJSONString(heartbeatDataCopy).hashCode();
    }

}
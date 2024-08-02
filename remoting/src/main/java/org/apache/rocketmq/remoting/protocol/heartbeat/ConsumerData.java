package org.apache.rocketmq.remoting.protocol.heartbeat;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
public class ConsumerData {

    private String groupName;

    private ConsumeType consumeType;

    private MessageModel messageModel;

    private ConsumeFromWhere consumeFromWhere;

    private Set<SubscriptionData> subscriptionDataSet = new HashSet<>();

    private boolean unitMode;

    @Override
    public String toString() {
        return "ConsumerData [groupName=" + groupName + ", consumeType=" + consumeType + ", messageModel=" + messageModel + ", consumeFromWhere=" + consumeFromWhere + ", unitMode=" + unitMode + ", subscriptionDataSet=" + subscriptionDataSet + "]";
    }

}
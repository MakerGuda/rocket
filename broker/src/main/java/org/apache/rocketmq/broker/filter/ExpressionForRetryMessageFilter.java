package org.apache.rocketmq.broker.filter;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.nio.ByteBuffer;
import java.util.Map;

@Getter
@Setter
public class ExpressionForRetryMessageFilter extends ExpressionMessageFilter {

    public ExpressionForRetryMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData, ConsumerFilterManager consumerFilterManager) {
        super(subscriptionData, consumerFilterData, consumerFilterManager);
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        if (subscriptionData == null) {
            return true;
        }
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }
        boolean isRetryTopic = subscriptionData.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;
        boolean decoded = false;
        if (isRetryTopic) {
            if (tempProperties == null && msgBuffer != null) {
                decoded = true;
                tempProperties = MessageDecoder.decodeProperties(msgBuffer);
            }
            assert tempProperties != null;
            String realTopic = tempProperties.get(MessageConst.PROPERTY_RETRY_TOPIC);
            String group = KeyBuilder.parseGroup(subscriptionData.getTopic());
            realFilterData = this.consumerFilterManager.get(realTopic, group);
        }
        if (realFilterData == null || realFilterData.getExpression() == null || realFilterData.getCompiledExpression() == null) {
            return true;
        }
        if (!decoded && tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }
        Object ret = null;
        try {
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);
            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }
        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);
        if (!(ret instanceof Boolean)) {
            return false;
        }
        return (Boolean) ret;
    }

}
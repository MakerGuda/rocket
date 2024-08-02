package org.apache.rocketmq.remoting.protocol.filter;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Arrays;

public class FilterAPI {

    /**
     * 通过主题和订阅字符串，构建订阅关系
     *
     * @param topic     主题
     * @param subString 订阅字符串
     * @return 订阅关系数据
     */
    public static SubscriptionData buildSubscriptionData(String topic, String subString) throws Exception {
        final SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        if (StringUtils.isEmpty(subString) || subString.equals(SubscriptionData.SUB_ALL)) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
            return subscriptionData;
        }
        //多个tags用两条竖线分开， eg: tag1 || tag2 || tag3
        String[] tags = subString.split("\\|\\|");
        if (tags.length > 0) {
            Arrays.stream(tags).map(String::trim).filter(tag -> !tag.isEmpty()).forEach(tag -> {
                subscriptionData.getTagsSet().add(tag);
                subscriptionData.getCodeSet().add(tag.hashCode());
            });
        } else {
            throw new Exception("subString split error");
        }
        return subscriptionData;
    }

    /**
     * 构建订阅关系
     */
    public static SubscriptionData buildSubscriptionData(String topic, String subString, String expressionType) throws Exception {
        final SubscriptionData subscriptionData = buildSubscriptionData(topic, subString);
        if (StringUtils.isNotBlank(expressionType)) {
            subscriptionData.setExpressionType(expressionType);
        }
        return subscriptionData;
    }

    /**
     * 构建订阅关系
     */
    public static SubscriptionData build(final String topic, final String subString, final String type) throws Exception {
        if (ExpressionType.TAG.equals(type) || type == null) {
            return buildSubscriptionData(topic, subString);
        }
        if (StringUtils.isEmpty(subString)) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);
        return subscriptionData;
    }

}
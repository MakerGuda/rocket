package org.apache.rocketmq.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.io.File;
import java.util.Properties;

import static org.apache.rocketmq.common.topic.TopicValidator.isTopicOrGroupIllegal;

/**
 * 通用校验
 */
public class Validators {

    public static final int CHARACTER_MAX_LENGTH = 255;

    public static final int TOPIC_MAX_LENGTH = 127;

    /**
     * 校验组相关
     */
    public static void checkGroup(String group) throws MQClientException {
        if (UtilAll.isBlank(group)) {
            throw new MQClientException("the specified group is blank", null);
        }
        if (group.length() > CHARACTER_MAX_LENGTH) {
            throw new MQClientException("the specified group is longer than group max length 255.", null);
        }
        if (isTopicOrGroupIllegal(group)) {
            throw new MQClientException(String.format("the specified group[%s] contains illegal characters, allowing only %s", group, "^[%|a-zA-Z0-9_-]+$"), null);
        }
    }

    /**
     * 校验消息
     */
    public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer) throws MQClientException {
        if (null == msg) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
        }
        Validators.checkTopic(msg.getTopic());
        //校验主题是否不允许发送消息
        Validators.isNotAllowedSendTopic(msg.getTopic());
        if (null == msg.getBody()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
        }
        if (0 == msg.getBody().length) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
        }
        if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
        }
        String lmqPath = msg.getUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.contains(lmqPath, File.separator)) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "INNER_MULTI_DISPATCH " + lmqPath + " can not contains " + File.separator + " character");
        }
    }

    /**
     * 校验主题
     */
    public static void checkTopic(String topic) throws MQClientException {
        if (UtilAll.isBlank(topic)) {
            throw new MQClientException("The specified topic is blank", null);
        }
        if (topic.length() > TOPIC_MAX_LENGTH) {
            throw new MQClientException(String.format("The specified topic is longer than topic max length %d.", TOPIC_MAX_LENGTH), null);
        }
        if (isTopicOrGroupIllegal(topic)) {
            throw new MQClientException(String.format("The specified topic[%s] contains illegal characters, allowing only %s", topic, "^[%|a-zA-Z0-9_-]+$"), null);
        }
    }

    /**
     * 判断当前主题是否为系统主题
     */
    public static void isSystemTopic(String topic) throws MQClientException {
        if (TopicValidator.isSystemTopic(topic)) {
            throw new MQClientException(String.format("The topic[%s] is conflict with system topic.", topic), null);
        }
    }

    /**
     * 校验当前主题是否不允许发送消息
     */
    public static void isNotAllowedSendTopic(String topic) throws MQClientException {
        if (TopicValidator.isNotAllowedSendTopic(topic)) {
            throw new MQClientException(String.format("Sending message to topic[%s] is forbidden.", topic), null);
        }
    }

    public static void checkTopicConfig(final TopicConfig topicConfig) throws MQClientException {
        if (!PermName.isValid(topicConfig.getPerm())) {
            throw new MQClientException(ResponseCode.NO_PERMISSION, String.format("topicPermission value: %s is invalid.", topicConfig.getPerm()));
        }
    }

    public static void checkBrokerConfig(final Properties brokerConfig) throws MQClientException {
        if (brokerConfig.containsKey("brokerPermission") && !PermName.isValid(brokerConfig.getProperty("brokerPermission"))) {
            throw new MQClientException(ResponseCode.NO_PERMISSION, String.format("brokerPermission value: %s is invalid.", brokerConfig.getProperty("brokerPermission")));
        }
    }

}
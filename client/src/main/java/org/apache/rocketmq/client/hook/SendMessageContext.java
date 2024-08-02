package org.apache.rocketmq.client.hook;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;

import java.util.Map;

@Getter
@Setter
public class SendMessageContext {

    /**
     * 生产者组
     */
    private String producerGroup;

    /**
     * 消息
     */
    private Message message;

    /**
     * 队列
     */
    private MessageQueue mq;

    /**
     * broker地址
     */
    private String brokerAddr;

    /**
     * 生产者地址
     */
    private String bornHost;

    /**
     * 消息发送模式 同步/异步/单向
     */
    private CommunicationMode communicationMode;

    /**
     * 发送结果
     */
    private SendResult sendResult;

    /**
     * 异常信息
     */
    private Exception exception;

    private Object mqTraceContext;

    /**
     * 属性
     */
    private Map<String, String> props;

    /**
     * 生产者
     */
    private DefaultMQProducerImpl producer;

    /**
     * 消息类型
     */
    private MessageType msgType = MessageType.Normal_Msg;

    /**
     * 命名空间
     */
    private String namespace;

}
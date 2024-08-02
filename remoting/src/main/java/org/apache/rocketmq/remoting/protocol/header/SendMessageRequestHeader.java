package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;

@Getter
@Setter
@RocketMQAction(value = RequestCode.SEND_MESSAGE, action = Action.PUB)
public class SendMessageRequestHeader extends TopicQueueRequestHeader {

    /**
     * 生产者组
     */
    @CFNotNull
    private String producerGroup;

    /**
     * 往哪个主题发送消息
     */
    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
    private String topic;

    /**
     * 默认主题
     */
    @CFNotNull
    private String defaultTopic;

    /**
     * 默认主题队列数目
     */
    @CFNotNull
    private Integer defaultTopicQueueNums;

    /**
     * 往哪个队列id发送消息
     */
    @CFNotNull
    private Integer queueId;

    /**
     * 系统标识
     */
    @CFNotNull
    private Integer sysFlag;

    /**
     * 消息产生时间
     */
    @CFNotNull
    private Long bornTimestamp;

    /**
     * 标识
     */
    @CFNotNull
    private Integer flag;

    /**
     * 消息属性
     */
    @CFNullable
    private String properties;

    /**
     * 重消费次数
     */
    @CFNullable
    private Integer reconsumeTimes;

    @CFNullable
    private Boolean unitMode;

    /**
     * 是否批量消息
     */
    @CFNullable
    private Boolean batch;

    /**
     * 最大重消费次数
     */
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Integer getReconsumeTimes() {
        if (null == reconsumeTimes) {
            return 0;
        }
        return reconsumeTimes;
    }

    public boolean isUnitMode() {
        if (null == unitMode) {
            return false;
        }
        return unitMode;
    }

    public boolean isBatch() {
        if (null == batch) {
            return false;
        }
        return batch;
    }

    public static SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
        SendMessageRequestHeaderV2 requestHeaderV2 = null;
        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
            case RequestCode.SEND_BATCH_MESSAGE:
            case RequestCode.SEND_MESSAGE_V2:
                requestHeaderV2 = request.decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            case RequestCode.SEND_MESSAGE:
                if (null == requestHeaderV2) {
                    requestHeader = request.decodeCommandCustomHeader(SendMessageRequestHeader.class);
                } else {
                    requestHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(requestHeaderV2);
                }
            default:
                break;
        }
        return requestHeader;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("producerGroup", producerGroup)
            .add("topic", topic)
            .add("defaultTopic", defaultTopic)
            .add("defaultTopicQueueNums", defaultTopicQueueNums)
            .add("queueId", queueId)
            .add("sysFlag", sysFlag)
            .add("bornTimestamp", bornTimestamp)
            .add("flag", flag)
            .add("properties", properties)
            .add("reconsumeTimes", reconsumeTimes)
            .add("unitMode", unitMode)
            .add("batch", batch)
            .add("maxReconsumeTimes", maxReconsumeTimes)
            .toString();
    }

}
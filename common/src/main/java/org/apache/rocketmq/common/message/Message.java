package org.apache.rocketmq.common.message;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class Message implements Serializable {

    private static final long serialVersionUID = 8445773977080406428L;

    /**
     * 消息所在主题
     */
    private String topic;

    /**
     * 消息flag
     */
    private int flag;

    /**
     * 扩展属性
     */
    private Map<String, String> properties;

    /**
     * 消息体
     */
    private byte[] body;

    /**
     * 事务id
     */
    private String transactionId;

    public Message() {
    }

    public Message(String topic, byte[] body) {
        this(topic, "", "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
        this.topic = topic;
        this.flag = flag;
        this.body = body;
        if (tags != null && !tags.isEmpty()) {
            this.setTags(tags);
        }
        if (keys != null && !keys.isEmpty()) {
            this.setKeys(keys);
        }
        this.setWaitStoreMsgOK(waitStoreMsgOK);
    }

    public Message(String topic, String tags, byte[] body) {
        this(topic, tags, "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, byte[] body) {
        this(topic, tags, keys, 0, body, true);
    }

    void putProperty(final String name, final String value) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }
        this.properties.put(name, value);
    }

    /**
     * 清除指定消息属性
     */
    void clearProperty(final String name) {
        if (null != this.properties) {
            this.properties.remove(name);
        }
    }

    /**
     * 设置用户自定义属性
     */
    public void putUserProperty(final String name, final String value) {
        if (MessageConst.STRING_HASH_SET.contains(name)) {
            throw new RuntimeException(String.format("The Property<%s> is used by system, input another please", name));
        }
        if (value == null || value.trim().isEmpty() || name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("The name or value of property can not be null or blank string!");
        }
        this.putProperty(name, value);
    }

    /**
     * 获取用户自定义属性
     */
    public String getUserProperty(final String name) {
        return this.getProperty(name);
    }

    /**
     * 获取指定属性
     */
    public String getProperty(final String name) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }
        return this.properties.get(name);
    }

    /**
     * 获取消息tags集合
     */
    public String getTags() {
        return this.getProperty(MessageConst.PROPERTY_TAGS);
    }

    /**
     * 设置消息tags
     */
    public void setTags(String tags) {
        this.putProperty(MessageConst.PROPERTY_TAGS, tags);
    }

    /**
     * 获取消息keys
     */
    public String getKeys() {
        return this.getProperty(MessageConst.PROPERTY_KEYS);
    }

    /**
     * 设置消息属性 keys
     */
    public void setKeys(String keys) {
        this.putProperty(MessageConst.PROPERTY_KEYS, keys);
    }

    /**
     * 设置消息keys，用空格进行分割
     */
    public void setKeys(Collection<String> keyCollection) {
        String keys = String.join(MessageConst.KEY_SEPARATOR, keyCollection);
        this.setKeys(keys);
    }

    /**
     * 获取消息的延迟级别
     */
    public int getDelayTimeLevel() {
        String t = this.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (t != null) {
            return Integer.parseInt(t);
        }
        return 0;
    }

    /**
     * 设置消息的延迟级别
     */
    public void setDelayTimeLevel(int level) {
        this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
    }

    public boolean isWaitStoreMsgOK() {
        String result = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        if (null == result) {
            return true;
        }
        return Boolean.parseBoolean(result);
    }

    /**
     * 消息发送时是否等消息存储完成再返回
     */
    public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
        this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, Boolean.toString(waitStoreMsgOK));
    }

    /**
     * 设置消息的实例id
     */
    public void setInstanceId(String instanceId) {
        this.putProperty(MessageConst.PROPERTY_INSTANCE_ID, instanceId);
    }

    public String getBuyerId() {
        return getProperty(MessageConst.PROPERTY_BUYER_ID);
    }

    public void setBuyerId(String buyerId) {
        putProperty(MessageConst.PROPERTY_BUYER_ID, buyerId);
    }

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", flag=" + flag +
                ", properties=" + properties +
                ", body=" + Arrays.toString(body) +
                ", transactionId='" + transactionId + '\'' +
                '}';
    }

    /**
     * 获取消息的延迟时间秒数
     */
    public long getDelayTimeSec() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }

    /**
     * 设置消息的延迟时间
     */
    public void setDelayTimeSec(long sec) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC, String.valueOf(sec));
    }

    /**
     * 获取消息的延迟时间，毫秒
     */
    public long getDelayTimeMs() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }

    public long getDeliverTimeMs() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }

    /**
     * 设置消息的延迟时间，毫秒
     */
    public void setDeliverTimeMs(long timeMs) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(timeMs));
    }

}
package org.apache.rocketmq.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.rocketmq.common.TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE;

@Getter
@Setter
public class TopicConfig {

    /**
     * 分隔符
     */
    private static final String SEPARATOR = " ";

    private static final TypeReference<Map<String, String>> ATTRIBUTES_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {
    };

    /**
     * 默认读队列数量
     */
    public static int defaultReadQueueNums = 16;

    /**
     * 默认写队列数量
     */
    public static int defaultWriteQueueNums = 16;

    /**
     * 主题名称
     */
    private String topicName;

    /**
     * 读队列数量
     */
    private int readQueueNums = defaultReadQueueNums;

    /**
     * 写队列数量
     */
    private int writeQueueNums = defaultWriteQueueNums;

    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;

    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;

    /**
     * 主题系统标识
     */
    private int topicSysFlag = 0;

    /**
     * 是否顺序主题
     */
    private boolean order = false;

    /**
     * 主题属性
     */
    private Map<String, String> attributes = new HashMap<>();

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm, int topicSysFlag) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
        this.topicSysFlag = topicSysFlag;
    }

    public TopicConfig(TopicConfig other) {
        this.topicName = other.topicName;
        this.readQueueNums = other.readQueueNums;
        this.writeQueueNums = other.writeQueueNums;
        this.perm = other.perm;
        this.topicFilterType = other.topicFilterType;
        this.topicSysFlag = other.topicSysFlag;
        this.order = other.order;
        this.attributes = other.attributes;
    }

    /**
     * 编码成字符串
     */
    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.topicName);
        sb.append(SEPARATOR);
        sb.append(this.readQueueNums);
        sb.append(SEPARATOR);
        sb.append(this.writeQueueNums);
        sb.append(SEPARATOR);
        sb.append(this.perm);
        sb.append(SEPARATOR);
        sb.append(this.topicFilterType);
        sb.append(SEPARATOR);
        if (attributes != null) {
            sb.append(JSON.toJSONString(attributes));
        }
        return sb.toString();
    }

    /**
     * 是否能够解码成功
     */
    public boolean decode(final String in) {
        String[] str = in.split(SEPARATOR);
        if (str.length >= 5) {
            this.topicName = str[0];
            this.readQueueNums = Integer.parseInt(str[1]);
            this.writeQueueNums = Integer.parseInt(str[2]);
            this.perm = Integer.parseInt(str[3]);
            this.topicFilterType = TopicFilterType.valueOf(str[4]);
            if (str.length >= 6) {
                try {
                    this.attributes = JSON.parseObject(str[5], ATTRIBUTES_TYPE_REFERENCE.getType());
                } catch (Exception ignore) {
                }
            }
            return true;
        }
        return false;
    }

    @JSONField(serialize = false, deserialize = false)
    public TopicMessageType getTopicMessageType() {
        if (attributes == null) {
            return TopicMessageType.NORMAL;
        }
        String content = attributes.get(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName());
        if (content == null) {
            return TopicMessageType.NORMAL;
        }
        return TopicMessageType.valueOf(content);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicConfig that = (TopicConfig) o;
        if (readQueueNums != that.readQueueNums) {
            return false;
        }
        if (writeQueueNums != that.writeQueueNums) {
            return false;
        }
        if (perm != that.perm) {
            return false;
        }
        if (topicSysFlag != that.topicSysFlag) {
            return false;
        }
        if (order != that.order) {
            return false;
        }
        if (!Objects.equals(topicName, that.topicName)) {
            return false;
        }
        if (topicFilterType != that.topicFilterType) {
            return false;
        }
        return Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        int result = topicName != null ? topicName.hashCode() : 0;
        result = 31 * result + readQueueNums;
        result = 31 * result + writeQueueNums;
        result = 31 * result + perm;
        result = 31 * result + (topicFilterType != null ? topicFilterType.hashCode() : 0);
        result = 31 * result + topicSysFlag;
        result = 31 * result + (order ? 1 : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

}
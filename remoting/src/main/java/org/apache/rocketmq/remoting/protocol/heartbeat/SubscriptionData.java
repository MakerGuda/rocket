package org.apache.rocketmq.remoting.protocol.heartbeat;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.filter.ExpressionType;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
public class SubscriptionData implements Comparable<SubscriptionData> {

    /**
     * 表示订阅全部
     */
    public final static String SUB_ALL = "*";

    private boolean classFilterMode = false;

    /**
     * 主题
     */
    private String topic;

    /**
     * 订阅关系字符串
     */
    private String subString;

    /**
     * tags集合
     */
    private Set<String> tagsSet = new HashSet<>();

    /**
     * tags对应的code集合
     */
    private Set<Integer> codeSet = new HashSet<>();

    private long subVersion = System.currentTimeMillis();

    /**
     * 表达式类型
     */
    private String expressionType = ExpressionType.TAG;

    @JSONField(serialize = false)
    private String filterClassSource;

    public SubscriptionData() {

    }

    public SubscriptionData(String topic, String subString) {
        super();
        this.topic = topic;
        this.subString = subString;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (classFilterMode ? 1231 : 1237);
        result = prime * result + ((codeSet == null) ? 0 : codeSet.hashCode());
        result = prime * result + ((subString == null) ? 0 : subString.hashCode());
        result = prime * result + ((tagsSet == null) ? 0 : tagsSet.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((expressionType == null) ? 0 : expressionType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionData other = (SubscriptionData) obj;
        if (classFilterMode != other.classFilterMode)
            return false;
        if (codeSet == null) {
            if (other.codeSet != null)
                return false;
        } else if (!codeSet.equals(other.codeSet))
            return false;
        if (subString == null) {
            if (other.subString != null)
                return false;
        } else if (!subString.equals(other.subString))
            return false;
        if (subVersion != other.subVersion)
            return false;
        if (tagsSet == null) {
            if (other.tagsSet != null)
                return false;
        } else if (!tagsSet.equals(other.tagsSet))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        if (expressionType == null) {
            return other.expressionType == null;
        } else return expressionType.equals(other.expressionType);
    }

    @Override
    public String toString() {
        return "SubscriptionData [classFilterMode=" + classFilterMode + ", topic=" + topic + ", subString=" + subString + ", tagsSet=" + tagsSet + ", codeSet=" + codeSet + ", subVersion=" + subVersion + ", expressionType=" + expressionType + "]";
    }

    @Override
    public int compareTo(SubscriptionData other) {
        String thisValue = this.topic + "@" + this.subString;
        String otherValue = other.topic + "@" + other.subString;
        return thisValue.compareTo(otherValue);
    }

}
package org.apache.rocketmq.remoting.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TopicList extends RemotingSerializable {

    /**
     * 主题名称列表
     */
    private Set<String> topicList = ConcurrentHashMap.newKeySet();

    /**
     * broker地址
     */
    private String brokerAddr;

    public Set<String> getTopicList() {
        return topicList;
    }

    public void setTopicList(Set<String> topicList) {
        this.topicList = topicList;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

}
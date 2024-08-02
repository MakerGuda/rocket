package org.apache.rocketmq.remoting.protocol.route;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

import java.util.*;

@Getter
@Setter
public class TopicRouteData extends RemotingSerializable {

    /**
     * 顺序主题配置
     */
    private String orderTopicConf;

    /**
     * 当前主题对应的队列集合
     */
    private List<QueueData> queueDatas;

    /**
     * 主题所在的broker集合
     */
    private List<BrokerData> brokerDatas;

    /**
     * key: brokerAddr value: 当前broker上的过滤器服务列表
     */
    private HashMap<String, List<String>> filterServerTable;

    /**
     * key: brokerName value: 主题队列映射信息
     */
    private Map<String, TopicQueueMappingInfo> topicQueueMappingByBroker;

    public TopicRouteData() {
        queueDatas = new ArrayList<>();
        brokerDatas = new ArrayList<>();
        filterServerTable = new HashMap<>();
    }

    public TopicRouteData(TopicRouteData topicRouteData) {
        this.queueDatas = new ArrayList<>();
        this.brokerDatas = new ArrayList<>();
        this.filterServerTable = new HashMap<>();
        this.orderTopicConf = topicRouteData.orderTopicConf;
        if (topicRouteData.queueDatas != null) {
            this.queueDatas.addAll(topicRouteData.queueDatas);
        }
        if (topicRouteData.brokerDatas != null) {
            this.brokerDatas.addAll(topicRouteData.brokerDatas);
        }
        if (topicRouteData.filterServerTable != null) {
            this.filterServerTable.putAll(topicRouteData.filterServerTable);
        }
        if (topicRouteData.topicQueueMappingByBroker != null) {
            this.topicQueueMappingByBroker = new HashMap<>(topicRouteData.topicQueueMappingByBroker);
        }
    }

    /**
     * 判断主题路由信息是否发生改变
     */
    public boolean topicRouteDataChanged(TopicRouteData oldData) {
        if (oldData == null)
            return true;
        TopicRouteData old = new TopicRouteData(oldData);
        TopicRouteData now = new TopicRouteData(this);
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerDatas == null) ? 0 : brokerDatas.hashCode());
        result = prime * result + ((orderTopicConf == null) ? 0 : orderTopicConf.hashCode());
        result = prime * result + ((queueDatas == null) ? 0 : queueDatas.hashCode());
        result = prime * result + ((filterServerTable == null) ? 0 : filterServerTable.hashCode());
        result = prime * result + ((topicQueueMappingByBroker == null) ? 0 : topicQueueMappingByBroker.hashCode());
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
        TopicRouteData other = (TopicRouteData) obj;
        if (brokerDatas == null) {
            if (other.brokerDatas != null)
                return false;
        } else if (!brokerDatas.equals(other.brokerDatas))
            return false;
        if (orderTopicConf == null) {
            if (other.orderTopicConf != null)
                return false;
        } else if (!orderTopicConf.equals(other.orderTopicConf))
            return false;
        if (queueDatas == null) {
            if (other.queueDatas != null)
                return false;
        } else if (!queueDatas.equals(other.queueDatas))
            return false;
        if (filterServerTable == null) {
            if (other.filterServerTable != null)
                return false;
        } else if (!filterServerTable.equals(other.filterServerTable))
            return false;
        if (topicQueueMappingByBroker == null) {
            return other.topicQueueMappingByBroker == null;
        } else return topicQueueMappingByBroker.equals(other.topicQueueMappingByBroker);
    }

    @Override
    public String toString() {
        return "TopicRouteData [orderTopicConf=" + orderTopicConf + ", queueDatas=" + queueDatas + ", brokerDatas=" + brokerDatas + ", filterServerTable=" + filterServerTable + ", topicQueueMappingInfoTable=" + topicQueueMappingByBroker + "]";
    }

}
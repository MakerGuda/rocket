package org.apache.rocketmq.remoting.rpc;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Getter
@Setter
public class ClientMetadata {

    /**
     * 主题路由信息 key: topic
     */
    private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    /**
     * key: topicName  value: {key: messageQueue value: brokerName}
     */
    private final ConcurrentMap<String, ConcurrentMap<MessageQueue, String>> topicEndPointsTable = new ConcurrentHashMap<>();

    /**
     * key: brokerName value: {key:brokerId value:address}
     */
    private final ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();

    /**
     * key: brokerName value: {key: address value: version}
     */
    private final ConcurrentMap<String, HashMap<String, Integer>> brokerVersionTable = new ConcurrentHashMap<>();

    public void freshTopicRoute(String topic, TopicRouteData topicRouteData) {
        if (topic == null
            || topicRouteData == null) {
            return;
        }
        TopicRouteData old = this.topicRouteTable.get(topic);
        if (!topicRouteData.topicRouteDataChanged(old)) {
            return ;
        }
        {
            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
            }
        }
        {
            ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, topicRouteData);
            if (!mqEndPoints.isEmpty()) {
                topicEndPointsTable.put(topic, mqEndPoints);
            }
        }
    }

    public String getBrokerNameFromMessageQueue(final MessageQueue mq) {
        if (topicEndPointsTable.get(mq.getTopic()) != null && !topicEndPointsTable.get(mq.getTopic()).isEmpty()) {
            return topicEndPointsTable.get(mq.getTopic()).get(mq);
        }
        return mq.getBrokerName();
    }

    /**
     * 刷新集群信息
     */
    public void refreshClusterInfo(ClusterInfo clusterInfo) {
        if (clusterInfo == null || clusterInfo.getBrokerAddrTable() == null) {
            return;
        }
        for (Map.Entry<String, BrokerData> entry : clusterInfo.getBrokerAddrTable().entrySet()) {
            brokerAddrTable.put(entry.getKey(), entry.getValue().getBrokerAddrs());
        }
    }

    public String findMasterBrokerAddr(String brokerName) {
        if (!brokerAddrTable.containsKey(brokerName)) {
            return null;
        }
        return brokerAddrTable.get(brokerName).get(MixAll.MASTER_ID);
    }

    public static ConcurrentMap<MessageQueue, String> topicRouteData2EndpointsForStaticTopic(final String topic, final TopicRouteData route) {
        if (route.getTopicQueueMappingByBroker() == null || route.getTopicQueueMappingByBroker().isEmpty()) {
            return new ConcurrentHashMap<>();
        }
        ConcurrentMap<MessageQueue, String> mqEndPointsOfBroker = new ConcurrentHashMap<>();
        Map<String, Map<String, TopicQueueMappingInfo>> mappingInfosByScope = new HashMap<>();
        for (Map.Entry<String, TopicQueueMappingInfo> entry : route.getTopicQueueMappingByBroker().entrySet()) {
            TopicQueueMappingInfo info = entry.getValue();
            String scope = info.getScope();
            if (scope != null) {
                if (!mappingInfosByScope.containsKey(scope)) {
                    mappingInfosByScope.put(scope, new HashMap<>());
                }
                mappingInfosByScope.get(scope).put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<String, Map<String, TopicQueueMappingInfo>> mapEntry : mappingInfosByScope.entrySet()) {
            String scope = mapEntry.getKey();
            Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap =  mapEntry.getValue();
            ConcurrentMap<MessageQueue, TopicQueueMappingInfo> mqEndPoints = new ConcurrentHashMap<>();
            List<Map.Entry<String, TopicQueueMappingInfo>> mappingInfos = new ArrayList<>(topicQueueMappingInfoMap.entrySet());
            mappingInfos.sort((o1, o2) -> (int) (o2.getValue().getEpoch() - o1.getValue().getEpoch()));
            int maxTotalNums = 0;
            long maxTotalNumOfEpoch = -1;
            for (Map.Entry<String, TopicQueueMappingInfo> entry : mappingInfos) {
                TopicQueueMappingInfo info = entry.getValue();
                if (info.getEpoch() >= maxTotalNumOfEpoch && info.getTotalQueues() > maxTotalNums) {
                    maxTotalNums = info.getTotalQueues();
                }
                for (Map.Entry<Integer, Integer> idEntry : entry.getValue().getCurrIdMap().entrySet()) {
                    int globalId = idEntry.getKey();
                    MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(info.getScope()), globalId);
                    TopicQueueMappingInfo oldInfo = mqEndPoints.get(mq);
                    if (oldInfo == null ||  oldInfo.getEpoch() <= info.getEpoch()) {
                        mqEndPoints.put(mq, info);
                    }
                }
            }


            //accomplish the static logic queues
            for (int i = 0; i < maxTotalNums; i++) {
                MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(scope), i);
                if (!mqEndPoints.containsKey(mq)) {
                    mqEndPointsOfBroker.put(mq, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST);
                } else {
                    mqEndPointsOfBroker.put(mq, mqEndPoints.get(mq).getBname());
                }
            }
        }
        return mqEndPointsOfBroker;
    }

}

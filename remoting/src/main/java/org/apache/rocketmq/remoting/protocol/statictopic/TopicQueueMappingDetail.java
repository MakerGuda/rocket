package org.apache.rocketmq.remoting.protocol.statictopic;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicQueueMappingDetail extends TopicQueueMappingInfo {

    /**
     * key: globalId
     */
    private final ConcurrentMap<Integer, List<LogicQueueMappingItem>> hostedQueues = new ConcurrentHashMap<>();

    public TopicQueueMappingDetail() {

    }

    public TopicQueueMappingDetail(String topic, int totalQueues, String bname, long epoch) {
        super(topic, totalQueues, bname, epoch);
    }

    public static void putMappingInfo(TopicQueueMappingDetail mappingDetail, Integer globalId, List<LogicQueueMappingItem> mappingInfo) {
        if (mappingInfo.isEmpty()) {
            return;
        }
        mappingDetail.hostedQueues.put(globalId, mappingInfo);
    }

    public static List<LogicQueueMappingItem> getMappingInfo(TopicQueueMappingDetail mappingDetail, Integer globalId) {
        return mappingDetail.hostedQueues.get(globalId);
    }

    public static ConcurrentMap<Integer, Integer> buildIdMap(TopicQueueMappingDetail mappingDetail, int level) {
        assert level == LEVEL_0;
        if (mappingDetail.hostedQueues.isEmpty()) {
            return new ConcurrentHashMap<>();
        }
        ConcurrentMap<Integer, Integer> tmpIdMap = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, List<LogicQueueMappingItem>> entry : mappingDetail.hostedQueues.entrySet()) {
            Integer globalId = entry.getKey();
            List<LogicQueueMappingItem> items = entry.getValue();
            if (!items.isEmpty()) {
                LogicQueueMappingItem curr = items.get(items.size() - 1);
                if (mappingDetail.bname.equals(curr.getBname())) {
                    tmpIdMap.put(globalId, curr.getQueueId());
                }
            }
        }
        return tmpIdMap;
    }

    public static long computeMaxOffsetFromMapping(TopicQueueMappingDetail mappingDetail, Integer globalId) {
        List<LogicQueueMappingItem> mappingItems = getMappingInfo(mappingDetail, globalId);
        if (mappingItems == null
                || mappingItems.isEmpty()) {
            return -1;
        }
        LogicQueueMappingItem item = mappingItems.get(mappingItems.size() - 1);
        return item.computeMaxStaticQueueOffset();
    }

    public static TopicQueueMappingInfo cloneAsMappingInfo(TopicQueueMappingDetail mappingDetail) {
        TopicQueueMappingInfo topicQueueMappingInfo = new TopicQueueMappingInfo(mappingDetail.topic, mappingDetail.totalQueues, mappingDetail.bname, mappingDetail.epoch);
        topicQueueMappingInfo.currIdMap = TopicQueueMappingDetail.buildIdMap(mappingDetail, LEVEL_0);
        return topicQueueMappingInfo;
    }

    public ConcurrentMap<Integer, List<LogicQueueMappingItem>> getHostedQueues() {
        return hostedQueues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicQueueMappingDetail)) return false;
        TopicQueueMappingDetail that = (TopicQueueMappingDetail) o;
        return new EqualsBuilder().append(hostedQueues, that.hostedQueues).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(hostedQueues).toHashCode();
    }

    @Override
    public String toString() {
        return "TopicQueueMappingDetail{" +
                "hostedQueues=" + hostedQueues +
                ", topic='" + topic + '\'' +
                ", totalQueues=" + totalQueues +
                ", bname='" + bname + '\'' +
                ", epoch=" + epoch +
                ", dirty=" + dirty +
                ", currIdMap=" + currIdMap +
                '}';
    }

}
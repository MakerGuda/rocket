package org.apache.rocketmq.remoting.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class TopicConfigAndMappingSerializeWrapper extends TopicConfigSerializeWrapper {

    /**
     * key: topic
     */
    private Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = new ConcurrentHashMap<>();

    /**
     * key: topic
     */
    private Map<String, TopicQueueMappingDetail> topicQueueMappingDetailMap = new ConcurrentHashMap<>();

    private DataVersion mappingDataVersion = new DataVersion();

    public static TopicConfigAndMappingSerializeWrapper from(TopicConfigSerializeWrapper wrapper) {
        if (wrapper instanceof TopicConfigAndMappingSerializeWrapper) {
            return (TopicConfigAndMappingSerializeWrapper) wrapper;
        }
        TopicConfigAndMappingSerializeWrapper mappingSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();
        mappingSerializeWrapper.setDataVersion(wrapper.getDataVersion());
        mappingSerializeWrapper.setTopicConfigTable(wrapper.getTopicConfigTable());
        return mappingSerializeWrapper;
    }

}
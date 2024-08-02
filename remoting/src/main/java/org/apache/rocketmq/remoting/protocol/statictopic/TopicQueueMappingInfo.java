package org.apache.rocketmq.remoting.protocol.statictopic;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicQueueMappingInfo extends RemotingSerializable {

    public static final int LEVEL_0 = 0;
    /**
     * key: logicId value:physicalId
     */
    protected ConcurrentMap<Integer, Integer> currIdMap = new ConcurrentHashMap<>();
    String topic;
    String scope = MixAll.METADATA_SCOPE_GLOBAL;
    int totalQueues;
    String bname;
    long epoch;
    boolean dirty;

    public TopicQueueMappingInfo() {

    }

    public TopicQueueMappingInfo(String topic, int totalQueues, String bname, long epoch) {
        this.topic = topic;
        this.totalQueues = totalQueues;
        this.bname = bname;
        this.epoch = epoch;
        this.dirty = false;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public int getTotalQueues() {
        return totalQueues;
    }

    public void setTotalQueues(int totalQueues) {
        this.totalQueues = totalQueues;
    }

    public String getBname() {
        return bname;
    }

    public void setBname(String bname) {
        this.bname = bname;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public ConcurrentMap<Integer, Integer> getCurrIdMap() {
        return currIdMap;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicQueueMappingInfo)) return false;
        TopicQueueMappingInfo info = (TopicQueueMappingInfo) o;
        if (totalQueues != info.totalQueues) return false;
        if (epoch != info.epoch) return false;
        if (dirty != info.dirty) return false;
        if (!Objects.equals(topic, info.topic)) return false;
        if (!Objects.equals(scope, info.scope)) return false;
        if (!Objects.equals(bname, info.bname)) return false;
        return Objects.equals(currIdMap, info.currIdMap);
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (scope != null ? scope.hashCode() : 0);
        result = 31 * result + totalQueues;
        result = 31 * result + (bname != null ? bname.hashCode() : 0);
        result = 31 * result + Long.hashCode(epoch);
        result = 31 * result + (dirty ? 1 : 0);
        result = 31 * result + (currIdMap != null ? currIdMap.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TopicQueueMappingInfo{" +
                "topic='" + topic + '\'' +
                ", scope='" + scope + '\'' +
                ", totalQueues=" + totalQueues +
                ", bname='" + bname + '\'' +
                ", epoch=" + epoch +
                ", dirty=" + dirty +
                ", currIdMap=" + currIdMap +
                '}';
    }

}
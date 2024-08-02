package org.apache.rocketmq.remoting.protocol.body;

import com.google.common.base.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class BrokerMemberGroup extends RemotingSerializable {

    /**
     * 集群名称
     */
    private String cluster;

    /**
     * broker名称
     */
    private String brokerName;

    /**
     * brokerId和broker地址的映射关系
     */
    private Map<Long, String> brokerAddrs;

    public BrokerMemberGroup() {
        this.brokerAddrs = new HashMap<>();
    }

    public BrokerMemberGroup(final String cluster, final String brokerName) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = new HashMap<>();
    }

    public long minimumBrokerId() {
        if (this.brokerAddrs.isEmpty()) {
            return 0;
        }
        return Collections.min(brokerAddrs.keySet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BrokerMemberGroup that = (BrokerMemberGroup) o;
        return Objects.equal(cluster, that.cluster) &&
            Objects.equal(brokerName, that.brokerName) &&
            Objects.equal(brokerAddrs, that.brokerAddrs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cluster, brokerName, brokerAddrs);
    }

    @Override
    public String toString() {
        return "BrokerMemberGroup{" +
            "cluster='" + cluster + '\'' +
            ", brokerName='" + brokerName + '\'' +
            ", brokerAddrs=" + brokerAddrs +
            '}';
    }

}
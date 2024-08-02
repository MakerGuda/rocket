package org.apache.rocketmq.client.trace;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.common.message.MessageClientIDSetter;

import java.util.List;

@Getter
@Setter
public class TraceContext implements Comparable<TraceContext> {

    private TraceType traceType;

    private long timeStamp = System.currentTimeMillis();

    private String regionId = "";

    private String regionName = "";

    private String groupName = "";

    private int costTime = 0;

    private boolean isSuccess = true;

    private String requestId = MessageClientIDSetter.createUniqID();

    private int contextCode = 0;

    private AccessChannel accessChannel;

    private List<TraceBean> traceBeans;

    @Override
    public int compareTo(TraceContext o) {
        return Long.compare(this.timeStamp, o.getTimeStamp());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("TraceContext{").append(traceType).append("_").append(groupName).append("_")
                .append(regionId).append("_").append(isSuccess).append("_");
        if (traceBeans != null && !traceBeans.isEmpty()) {
            for (TraceBean bean : traceBeans) {
                sb.append(bean.getMsgId()).append("_").append(bean.getTopic()).append("_");
            }
        }
        sb.append('}');
        return sb.toString();
    }

}
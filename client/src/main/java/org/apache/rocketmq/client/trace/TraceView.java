package org.apache.rocketmq.client.trace;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class TraceView {

    private String msgId;

    private String tags;

    private String keys;

    private String storeHost;

    private String clientHost;

    private int costTime;

    private String msgType;

    private String offSetMsgId;

    private long timeStamp;

    private long bornTime;

    private String topic;

    private String groupName;

    private String status;

    public static List<TraceView> decodeFromTraceTransData(String key, MessageExt messageExt) {
        List<TraceView> messageTraceViewList = new ArrayList<>();
        String messageBody = new String(messageExt.getBody(), StandardCharsets.UTF_8);
        if (messageBody.isEmpty()) {
            return messageTraceViewList;
        }
        List<TraceContext> traceContextList = TraceDataEncoder.decoderFromTraceDataString(messageBody);
        for (TraceContext context : traceContextList) {
            TraceView messageTraceView = new TraceView();
            TraceBean traceBean = context.getTraceBeans().get(0);
            if (!traceBean.getMsgId().equals(key)) {
                continue;
            }
            messageTraceView.setCostTime(context.getCostTime());
            messageTraceView.setGroupName(context.getGroupName());
            if (context.isSuccess()) {
                messageTraceView.setStatus("success");
            } else {
                messageTraceView.setStatus("failed");
            }
            messageTraceView.setKeys(traceBean.getKeys());
            messageTraceView.setMsgId(traceBean.getMsgId());
            messageTraceView.setTags(traceBean.getTags());
            messageTraceView.setTopic(traceBean.getTopic());
            messageTraceView.setMsgType(context.getTraceType().name());
            messageTraceView.setOffSetMsgId(traceBean.getOffsetMsgId());
            messageTraceView.setTimeStamp(context.getTimeStamp());
            messageTraceView.setStoreHost(traceBean.getStoreHost());
            messageTraceView.setClientHost(messageExt.getBornHostString());
            messageTraceViewList.add(messageTraceView);
        }
        return messageTraceViewList;
    }

}
package org.apache.rocketmq.client.trace.hook;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ConsumeMessageTraceHookImpl implements ConsumeMessageHook {

    private TraceDispatcher localDispatcher;

    public ConsumeMessageTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "ConsumeMessageTraceHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        TraceContext traceContext = new TraceContext();
        context.setMqTraceContext(traceContext);
        traceContext.setTraceType(TraceType.SubBefore);
        traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getConsumerGroup()));
        List<TraceBean> beans = new ArrayList<>();
        for (MessageExt msg : context.getMsgList()) {
            if (msg == null) {
                continue;
            }
            String regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
            String traceOn = msg.getProperty(MessageConst.PROPERTY_TRACE_SWITCH);
            if (traceOn != null && traceOn.equals("false")) {
                continue;
            }
            TraceBean traceBean = new TraceBean();
            traceBean.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic()));
            traceBean.setMsgId(msg.getMsgId());
            traceBean.setTags(msg.getTags());
            traceBean.setKeys(msg.getKeys());
            traceBean.setStoreTime(msg.getStoreTimestamp());
            traceBean.setBodyLength(msg.getStoreSize());
            traceBean.setRetryTimes(msg.getReconsumeTimes());
            traceContext.setRegionId(regionId);
            beans.add(traceBean);
        }
        if (!beans.isEmpty()) {
            traceContext.setTraceBeans(beans);
            traceContext.setTimeStamp(System.currentTimeMillis());
            localDispatcher.append(traceContext);
        }
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        TraceContext subBeforeContext = (TraceContext) context.getMqTraceContext();
        if (subBeforeContext.getTraceBeans() == null || subBeforeContext.getTraceBeans().isEmpty()) {
            return;
        }
        TraceContext subAfterContext = new TraceContext();
        subAfterContext.setTraceType(TraceType.SubAfter);
        subAfterContext.setRegionId(subBeforeContext.getRegionId());
        subAfterContext.setGroupName(NamespaceUtil.withoutNamespace(subBeforeContext.getGroupName()));
        subAfterContext.setRequestId(subBeforeContext.getRequestId());
        subAfterContext.setAccessChannel(context.getAccessChannel());
        subAfterContext.setSuccess(context.isSuccess());
        int costTime = (int) ((System.currentTimeMillis() - subBeforeContext.getTimeStamp()) / context.getMsgList().size());
        subAfterContext.setCostTime(costTime);
        subAfterContext.setTraceBeans(subBeforeContext.getTraceBeans());
        Map<String, String> props = context.getProps();
        if (props != null) {
            String contextType = props.get(MixAll.CONSUME_CONTEXT_TYPE);
            if (contextType != null) {
                subAfterContext.setContextCode(ConsumeReturnType.valueOf(contextType).ordinal());
            }
        }
        localDispatcher.append(subAfterContext);
    }

}
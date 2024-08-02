package org.apache.rocketmq.client.trace.hook;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.trace.*;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;

import java.util.ArrayList;

@Getter
@Setter
public class SendMessageTraceHookImpl implements SendMessageHook {

    private TraceDispatcher localDispatcher;

    public SendMessageTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "SendMessageTraceHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
            return;
        }
        TraceContext traceContext = new TraceContext();
        traceContext.setTraceBeans(new ArrayList<>(1));
        context.setMqTraceContext(traceContext);
        traceContext.setTraceType(TraceType.Pub);
        traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
        traceBean.setTags(context.getMessage().getTags());
        traceBean.setKeys(context.getMessage().getKeys());
        traceBean.setStoreHost(context.getBrokerAddr());
        traceBean.setBodyLength(context.getMessage().getBody().length);
        traceBean.setMsgType(context.getMsgType());
        traceContext.getTraceBeans().add(traceBean);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName()) || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }
        if (context.getSendResult().getRegionId() == null || !context.getSendResult().isTraceOn()) {
            return;
        }
        TraceContext traceContext = (TraceContext) context.getMqTraceContext();
        TraceBean traceBean = traceContext.getTraceBeans().get(0);
        int costTime = (int) ((System.currentTimeMillis() - traceContext.getTimeStamp()) / traceContext.getTraceBeans().size());
        traceContext.setCostTime(costTime);
        traceContext.setSuccess(context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK));
        traceContext.setRegionId(context.getSendResult().getRegionId());
        traceBean.setMsgId(context.getSendResult().getMsgId());
        traceBean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
        traceBean.setStoreTime(traceContext.getTimeStamp() + costTime / 2);
        localDispatcher.append(traceContext);
    }

}
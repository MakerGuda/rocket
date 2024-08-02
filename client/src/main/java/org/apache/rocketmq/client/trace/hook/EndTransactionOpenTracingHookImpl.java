package org.apache.rocketmq.client.trace.hook;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.trace.TraceConstants;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageType;

@Getter
@Setter
public class EndTransactionOpenTracingHookImpl implements EndTransactionHook {

    private Tracer tracer;

    public EndTransactionOpenTracingHookImpl(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public String hookName() {
        return "EndTransactionOpenTracingHook";
    }

    @Override
    public void endTransaction(EndTransactionContext context) {
        if (context == null) {
            return;
        }
        Message msg = context.getMessage();
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(TraceConstants.END_TRANSACTION).withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_PRODUCER);
        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(msg.getProperties()));
        if (spanContext != null) {
            spanBuilder.asChildOf(spanContext);
        }
        Span span = spanBuilder.start();
        span.setTag(Tags.PEER_SERVICE, TraceConstants.ROCKETMQ_SERVICE);
        span.setTag(Tags.MESSAGE_BUS_DESTINATION, msg.getTopic());
        span.setTag(TraceConstants.ROCKETMQ_TAGS, msg.getTags());
        span.setTag(TraceConstants.ROCKETMQ_KEYS, msg.getKeys());
        span.setTag(TraceConstants.ROCKETMQ_STORE_HOST, context.getBrokerAddr());
        span.setTag(TraceConstants.ROCKETMQ_MSG_ID, context.getMsgId());
        span.setTag(TraceConstants.ROCKETMQ_MSG_TYPE, MessageType.Trans_msg_Commit.name());
        span.setTag(TraceConstants.ROCKETMQ_TRANSACTION_ID, context.getTransactionId());
        span.setTag(TraceConstants.ROCKETMQ_TRANSACTION_STATE, context.getTransactionState().name());
        span.setTag(TraceConstants.ROCKETMQ_IS_FROM_TRANSACTION_CHECK, context.isFromTransactionCheck());
        span.finish();
    }

}
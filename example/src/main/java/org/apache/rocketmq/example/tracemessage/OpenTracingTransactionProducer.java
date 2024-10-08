package org.apache.rocketmq.example.tracemessage;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.trace.hook.EndTransactionOpenTracingHookImpl;
import org.apache.rocketmq.client.trace.hook.SendMessageOpenTracingHookImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;

public class OpenTracingTransactionProducer {

    public static final String PRODUCER_GROUP = "please_rename_unique_group_name";

    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";

    public static final String TOPIC = "TopicTest";

    public static final String TAG = "Tag";

    public static final String KEY = "KEY";

    public static final int MESSAGE_COUNT = 100000;

    public static void main(String[] args) throws MQClientException, InterruptedException {
        Tracer tracer = initTracer();
        TransactionMQProducer producer = new TransactionMQProducer(PRODUCER_GROUP);
        producer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageOpenTracingHookImpl(tracer));
        producer.getDefaultMQProducerImpl().registerEndTransactionHook(new EndTransactionOpenTracingHookImpl(tracer));
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();

        try {
            Message msg = new Message(TOPIC, TAG, KEY, "Hello RocketMQ".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
            System.out.printf("%s%n", sendResult);
        } catch (MQClientException | UnsupportedEncodingException ignore) {
        }
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }

    private static Tracer initTracer() {
        Configuration.SamplerConfiguration samplerConfig = Configuration.SamplerConfiguration.fromEnv().withType(ConstSampler.TYPE).withParam(1);
        Configuration.ReporterConfiguration reporterConfig = Configuration.ReporterConfiguration.fromEnv().withLogSpans(true);
        Configuration config = new Configuration("rocketmq").withSampler(samplerConfig).withReporter(reporterConfig);
        GlobalTracer.registerIfAbsent(config.getTracer());
        return config.getTracer();
    }

}
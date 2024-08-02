package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;

import java.util.List;
import java.util.concurrent.ExecutorService;


@Getter
@Setter
public class TransactionMQProducer extends DefaultMQProducer {

    private TransactionCheckListener transactionCheckListener;

    private int checkThreadPoolMinSize = 1;

    private int checkThreadPoolMaxSize = 1;

    private int checkRequestHoldMax = 2000;

    private ExecutorService executorService;

    private TransactionListener transactionListener;

    public TransactionMQProducer() {
    }

    public TransactionMQProducer(final String producerGroup) {
        super(producerGroup);
    }

    public TransactionMQProducer(final String producerGroup, final List<String> topics) {
        super(producerGroup, null, topics);
    }

    public TransactionMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace, final String customizedTraceTopic) {
        super(producerGroup, rpcHook, enableMsgTrace, customizedTraceTopic);
    }

    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.initTransactionEnv();
        super.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.defaultMQProducerImpl.destroyTransactionEnv();
    }

    @Override
    public TransactionSendResult sendMessageInTransaction(final Message msg,
                                                          final Object arg) throws MQClientException {
        if (null == this.transactionListener) {
            throw new MQClientException("TransactionListener is null", null);
        }
        msg.setTopic(NamespaceUtil.wrapNamespace(this.getNamespace(), msg.getTopic()));
        return this.defaultMQProducerImpl.sendMessageInTransaction(msg, null, arg);
    }

}
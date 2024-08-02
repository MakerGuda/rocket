package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.EndTransactionTraceHookImpl;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.compression.CompressionType;
import org.apache.rocketmq.common.compression.Compressor;
import org.apache.rocketmq.common.compression.CompressorFactory;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 消息生产者实现类
 */
@Getter
@Setter
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    /**
     * 消息生产的内部实现
     */
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;

    private final Logger logger = LoggerFactory.getLogger(DefaultMQProducer.class);

    /**
     * 重试的响应码集合
     */
    private final Set<Integer> retryResponseCodes = new CopyOnWriteArraySet<>(Arrays.asList(
            ResponseCode.TOPIC_NOT_EXIST,
            ResponseCode.SERVICE_NOT_AVAILABLE,
            ResponseCode.SYSTEM_ERROR,
            ResponseCode.SYSTEM_BUSY,
            ResponseCode.NO_PERMISSION,
            ResponseCode.NO_BUYER_ID,
            ResponseCode.NOT_IN_CURRENT_UNIT
    ));

    /**
     * 消息生产者组
     */
    private String producerGroup;

    /**
     * 事务生产者需要初始化的主题列表
     */
    private List<String> topics;

    private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;

    /**
     * 每个主题创建时默认的队列数量
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * 发送消息的超时时间
     */
    private int sendMsgTimeout = 3000;

    /**
     * 压缩消息的消息体阈值，超过当前值大小的消息将会被压缩, 默认4Kb
     */
    private int compressMsgBodyOverHowMuch = 1024 * 4;

    /**
     * 同步发送模式下，发送失败时的最大重试次数
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * 异步发送模式下，发送失败时的最大重试次数
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * 发送失败时，是否重试其他broker
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * 发送消息体的最大大小
     */
    private int maxMessageSize = 1024 * 1024 * 4;

    private TraceDispatcher traceDispatcher = null;

    /**
     * 是否批量消息
     */
    private boolean autoBatch = false;

    private ProduceAccumulator produceAccumulator = null;

    /**
     * 异步发送消息繁忙时，是否阻塞消息
     */
    private boolean enableBackpressureForAsyncMode = false;

    /**
     * 异步发送消息繁忙时，同时发送的最大数量
     */
    private int backPressureForAsyncSendNum = 10000;

    /**
     * 异步发送消息繁忙时，限制消息大小
     */
    private int backPressureForAsyncSendSize = 100 * 1024 * 1024;

    private RPCHook rpcHook;

    /**
     * 压缩级别
     */
    private int compressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));

    /**
     * 压缩方式
     */
    private CompressionType compressType = CompressionType.of(System.getProperty(MixAll.MESSAGE_COMPRESS_TYPE, "ZLIB"));

    /**
     * 压缩算法
     */
    private Compressor compressor = CompressorFactory.getCompressor(compressType);

    public DefaultMQProducer() {
        this(MixAll.DEFAULT_PRODUCER_GROUP);
    }

    public DefaultMQProducer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }

    public DefaultMQProducer(final String producerGroup) {
        this(producerGroup, null);
    }

    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
        this(producerGroup, rpcHook, null);
    }

    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook, final List<String> topics) {
        this(producerGroup, rpcHook, topics, false, null);
    }

    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(producerGroup, null, enableMsgTrace, customizedTraceTopic);
    }

    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(producerGroup, rpcHook, null, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * 构造函数
     *
     * @param producerGroup        生产者组
     * @param rpcHook              rpcHook
     * @param topics               主题列表
     * @param enableMsgTrace       是否允许消息追踪
     * @param customizedTraceTopic 自定义追踪主题
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook, final List<String> topics, boolean enableMsgTrace, final String customizedTraceTopic) {
        this.producerGroup = producerGroup;
        this.rpcHook = rpcHook;
        this.topics = topics;
        this.enableTrace = enableMsgTrace;
        this.traceTopic = customizedTraceTopic;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        produceAccumulator = MQClientManager.getInstance().getOrCreateProduceAccumulator(this);
    }

    /**
     * 启动生产者实例
     */
    @Override
    public void start() throws MQClientException {
        this.setProducerGroup(withNamespace(this.producerGroup));
        this.defaultMQProducerImpl.start();
        if (this.produceAccumulator != null) {
            this.produceAccumulator.start();
        }
        if (enableTrace) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, traceTopic, rpcHook);
                dispatcher.setHostProducer(this.defaultMQProducerImpl);
                dispatcher.setNamespaceV2(this.namespaceV2);
                traceDispatcher = dispatcher;
                this.defaultMQProducerImpl.registerSendMessageHook(new SendMessageTraceHookImpl(traceDispatcher));
                this.defaultMQProducerImpl.registerEndTransactionHook(new EndTransactionTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                logger.error("system mq trace hook init failed ,maybe can't send msg trace data");
            }
        }
        if (null != traceDispatcher) {
            if (traceDispatcher instanceof AsyncTraceDispatcher) {
                ((AsyncTraceDispatcher) traceDispatcher).getTraceProducer().setUseTLS(isUseTLS());
            }
            try {
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                logger.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * 管理生产者实例和相关资源
     */
    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
        if (this.produceAccumulator != null) {
            this.produceAccumulator.shutdown();
        }
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    /**
     * 获取主题下的队列列表
     */
    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(withNamespace(topic));
    }

    private boolean canBatch(Message msg) {
        if (!produceAccumulator.tryAddMessage(msg)) {
            return false;
        }
        if (msg.getDelayTimeLevel() > 0 || msg.getDelayTimeMs() > 0 || msg.getDelayTimeSec() > 0 || msg.getDeliverTimeMs() > 0) {
            return false;
        }
        if (msg.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            return false;
        }
        return !msg.getProperties().containsKey(MessageConst.PROPERTY_PRODUCER_GROUP);
    }

    /**
     * 同步模式消息发送
     */
    @Override
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        if (this.getAutoBatch() && !(msg instanceof MessageBatch)) {
            return sendByAccumulator(msg, null, null);
        } else {
            return sendDirect(msg, null, null);
        }
    }

    /**
     * 同步模式消息发送
     */
    @Override
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    /**
     * 异步消息发送
     *
     * @param msg          待发送消息
     * @param sendCallback 回调函数
     */
    @Override
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        try {
            if (this.getAutoBatch() && !(msg instanceof MessageBatch)) {
                sendByAccumulator(msg, null, sendCallback);
            } else {
                sendDirect(msg, null, sendCallback);
            }
        } catch (Throwable e) {
            sendCallback.onException(e);
        }
    }

    /**
     * 异步消息发送
     *
     * @param msg          待发送消息
     * @param sendCallback 回调函数
     * @param timeout      超时时间
     */
    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
    }

    /**
     * 单向消息发送，只发送一次，不管消息是否发送成功
     */
    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg);
    }

    /**
     * 同步发送消息，指定发送的mq
     *
     * @param msg 待发送消息
     * @param mq  目标mq
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        mq = queueWithNamespace(mq);
        if (this.getAutoBatch() && !(msg instanceof MessageBatch)) {
            return sendByAccumulator(msg, mq, null);
        } else {
            return sendDirect(msg, mq, null);
        }
    }

    /**
     * 异步发送消息，指定mq和超时时间
     *
     * @param msg     待发送消息
     * @param mq      目标mq
     * @param timeout 超时时间
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), timeout);
    }

    /**
     * 异步发送消息，指定mq
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        mq = queueWithNamespace(mq);
        try {
            if (this.getAutoBatch() && !(msg instanceof MessageBatch)) {
                sendByAccumulator(msg, mq, sendCallback);
            } else {
                sendDirect(msg, mq, sendCallback);
            }
        } catch (MQBrokerException ignored) {
        }
    }

    /**
     * 异步发送消息，指定mq，回调函数和超时时间
     *
     * @param msg          待发送消息
     * @param mq           目标mq
     * @param sendCallback 回调函数
     * @param timeout      超时时间
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback, timeout);
    }

    /**
     * 指定mq发送单向消息
     */
    @Override
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, queueWithNamespace(mq));
    }

    /**
     * 指定mq选择器，同步发送消息
     *
     * @param msg      待发送消息
     * @param selector mq选择器
     * @param arg      mq选择器相关参数
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        MessageQueue mq = this.defaultMQProducerImpl.invokeMessageQueueSelector(msg, selector, arg, this.getSendMsgTimeout());
        mq = queueWithNamespace(mq);
        if (this.getAutoBatch() && !(msg instanceof MessageBatch)) {
            return sendByAccumulator(msg, mq, null);
        } else {
            return sendDirect(msg, mq, null);
        }
    }

    /**
     * 指定mq选择器同步发送消息
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }

    /**
     * 指定mq选择器，异步发送消息
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        try {
            MessageQueue mq = this.defaultMQProducerImpl.invokeMessageQueueSelector(msg, selector, arg, this.getSendMsgTimeout());
            mq = queueWithNamespace(mq);
            if (this.getAutoBatch() && !(msg instanceof MessageBatch)) {
                sendByAccumulator(msg, mq, sendCallback);
            } else {
                sendDirect(msg, mq, sendCallback);
            }
        } catch (Throwable e) {
            sendCallback.onException(e);
        }
    }

    /**
     * 指定mq选择器和超时时间，异步发送消息
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
    }

    /**
     * 消息发送
     */
    public SendResult sendDirect(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //同步模式发送
        if (sendCallback == null) {
            if (mq == null) {
                return this.defaultMQProducerImpl.send(msg);
            } else {
                return this.defaultMQProducerImpl.send(msg, mq);
            }
        } else {
            //异步模式发送
            if (mq == null) {
                this.defaultMQProducerImpl.send(msg, sendCallback);
            } else {
                this.defaultMQProducerImpl.send(msg, mq, sendCallback);
            }
            return null;
        }
    }

    public SendResult sendByAccumulator(Message msg, MessageQueue mq,
                                        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        if (!canBatch(msg)) {
            return sendDirect(msg, mq, sendCallback);
        } else {
            Validators.checkMessage(msg, this);
            MessageClientIDSetter.setUniqID(msg);
            if (sendCallback == null) {
                return this.produceAccumulator.send(msg, mq, this);
            } else {
                this.produceAccumulator.send(msg, mq, sendCallback, this);
                return null;
            }
        }
    }

    /**
     * Send request message in synchronous mode. This method returns only when the consumer consume the request message and reply a message. </p>
     */
    @Override
    public Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, timeout);
    }

    /**
     * Request asynchronously.
     */
    @Override
    public void request(final Message msg, final RequestCallback requestCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, requestCallback, timeout);
    }

    /**
     * Same to {@link #request(Message, long)}  with message queue selector specified.
     */
    @Override
    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #request(Message, RequestCallback, long)} with target message selector specified.
     */
    @Override
    public void request(final Message msg, final MessageQueueSelector selector, final Object arg, final RequestCallback requestCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, selector, arg, requestCallback, timeout);
    }

    /**
     * Same to {@link #request(Message, long)}  with target message queue specified in addition.
     */
    @Override
    public Message request(final Message msg, final MessageQueue mq, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, mq, timeout);
    }

    /**
     * Same to {@link #request(Message, RequestCallback, long)} with target message queue specified.
     */
    @Override
    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, mq, requestCallback, timeout);
    }

    /**
     * 指定mq选择器，单向发送消息
     */
    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }

    /**
     * 发送事务消息
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, Object arg) throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * 创建主题
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum,
                            Map<String, String> attributes) throws MQClientException {
        createTopic(key, withNamespace(newTopic), queueNum, 0, null);
    }

    /**
     * 在broker上创建主题
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag, Map<String, String> attributes) throws MQClientException {
        this.defaultMQProducerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    /**
     * 通过给定时间戳查询队列的消费偏移量
     */
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * 查询指定消息的最大偏移量
     */
    @Deprecated
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * 查询指定消息队列的最小偏移量
     */
    @Deprecated
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * 查询最早一次消息的存储时间
     */
    @Deprecated
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    /**
     * 通过key查询消息
     */
    @Deprecated
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException, InterruptedException {
        return this.defaultMQProducerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    /**
     * 通过指定的消息id查询消息
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            return this.defaultMQProducerImpl.viewMessage(topic, msgId);
        } catch (Exception ignored) {
        }
        return this.defaultMQProducerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
    }

    /**
     * 同步发送批量消息
     */
    @Override
    public SendResult send(Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs));
    }

    /**
     * 同步发送批量消息
     */
    @Override
    public SendResult send(Collection<Message> msgs, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), timeout);
    }

    /**
     * 指定mq，同步发送批量消息
     */
    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue);
    }

    /**
     * 指定mq，同步发送批量消息
     */
    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout);
    }

    /**
     * 异步发送批量消息
     */
    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), sendCallback);
    }

    /**
     * 异步发送批量消息
     */
    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), sendCallback, timeout);
    }

    /**
     * 指定mq，异步发送批量消息
     */
    @Override
    public void send(Collection<Message> msgs, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback);
    }

    /**
     * 指定mq，异步发送批量消息
     */
    @Override
    public void send(Collection<Message> msgs, MessageQueue mq, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback, timeout);
    }

    /**
     * 将消息列表转换为批量消息
     */
    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, this);
                MessageClientIDSetter.setUniqID(message);
                message.setTopic(withNamespace(message.getTopic()));
            }
            MessageClientIDSetter.setUniqID(msgBatch);
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        msgBatch.setTopic(withNamespace(msgBatch.getTopic()));
        return msgBatch;
    }

    public boolean getAutoBatch() {
        if (this.produceAccumulator == null) {
            return false;
        }
        return this.autoBatch;
    }

    @Override
    public void setStartDetectorEnable(boolean startDetectorEnable) {
        super.setStartDetectorEnable(startDetectorEnable);
        this.defaultMQProducerImpl.getMqFaultStrategy().setStartDetectorEnable(startDetectorEnable);
    }

    public void setCompressType(CompressionType compressType) {
        this.compressType = compressType;
        this.compressor = CompressorFactory.getCompressor(compressType);
    }

}
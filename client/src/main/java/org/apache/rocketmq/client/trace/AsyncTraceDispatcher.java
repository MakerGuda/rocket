package org.apache.rocketmq.client.trace;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.client.trace.TraceConstants.TRACE_INSTANCE_NAME;

@Getter
@Setter
public class AsyncTraceDispatcher implements TraceDispatcher {

    private final static Logger log = LoggerFactory.getLogger(AsyncTraceDispatcher.class);

    private final static AtomicInteger COUNTER = new AtomicInteger();

    private final static short MAX_MSG_KEY_SIZE = Short.MAX_VALUE - 10000;

    private final int queueSize;

    private final int batchSize;

    private final int maxMsgSize;

    private final long pollingTimeMil;

    private final long waitTimeThresholdMil;

    private final DefaultMQProducer traceProducer;

    private final ThreadPoolExecutor traceExecutor;

    private AtomicLong discardCount;

    private Thread worker;

    private final ArrayBlockingQueue<TraceContext> traceContextQueue;

    private final HashMap<String, TraceDataSegment> taskQueueByTopic;

    private ArrayBlockingQueue<Runnable> appenderQueue;

    private volatile Thread shutDownHook;

    private volatile boolean stopped = false;

    private DefaultMQProducerImpl hostProducer;

    private DefaultMQPushConsumerImpl hostConsumer;

    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    private String dispatcherId = UUID.randomUUID().toString();

    private volatile String traceTopicName;

    private AtomicBoolean isStarted = new AtomicBoolean(false);

    private volatile AccessChannel accessChannel = AccessChannel.LOCAL;

    private String group;

    private Type type;

    private String namespaceV2;

    public AsyncTraceDispatcher(String group, Type type, String traceTopicName, RPCHook rpcHook) {
        this.queueSize = 2048;
        this.batchSize = 100;
        this.maxMsgSize = 128000;
        this.pollingTimeMil = 100;
        this.waitTimeThresholdMil = 500;
        this.discardCount = new AtomicLong(0L);
        this.traceContextQueue = new ArrayBlockingQueue<>(1024);
        this.taskQueueByTopic = new HashMap<>();
        this.group = group;
        this.type = type;
        this.appenderQueue = new ArrayBlockingQueue<>(queueSize);
        if (!UtilAll.isBlank(traceTopicName)) {
            this.traceTopicName = traceTopicName;
        } else {
            this.traceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
        }
        this.traceExecutor = new ThreadPoolExecutor(10, 20, 1000 * 60, TimeUnit.MILLISECONDS, this.appenderQueue, new ThreadFactoryImpl("MQTraceSendThread_"));
        traceProducer = getAndCreateTraceProducer(rpcHook);
    }

    @Override
    public void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException {
        if (isStarted.compareAndSet(false, true)) {
            traceProducer.setNamesrvAddr(nameSrvAddr);
            traceProducer.setInstanceName(TRACE_INSTANCE_NAME + "_" + nameSrvAddr);
            traceProducer.setNamespaceV2(namespaceV2);
            traceProducer.setEnableTrace(false);
            traceProducer.start();
        }
        this.accessChannel = accessChannel;
        this.worker = new Thread(new AsyncRunnable(), "MQ-AsyncTraceDispatcher-Thread-" + dispatcherId);
        this.worker.setDaemon(true);
        this.worker.start();
        this.registerShutDownHook();
    }

    private DefaultMQProducer getAndCreateTraceProducer(RPCHook rpcHook) {
        DefaultMQProducer traceProducerInstance = this.traceProducer;
        if (traceProducerInstance == null) {
            traceProducerInstance = new DefaultMQProducer(rpcHook);
            traceProducerInstance.setProducerGroup(genGroupNameForTrace());
            traceProducerInstance.setSendMsgTimeout(5000);
            traceProducerInstance.setVipChannelEnabled(false);
            traceProducerInstance.setMaxMessageSize(maxMsgSize);
        }
        return traceProducerInstance;
    }

    private String genGroupNameForTrace() {
        return TraceConstants.GROUP_NAME_PREFIX + "-" + this.group + "-" + this.type + "-" + COUNTER.incrementAndGet();
    }

    @Override
    public boolean append(final Object ctx) {
        boolean result = traceContextQueue.offer((TraceContext) ctx);
        if (!result) {
            log.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    @Override
    public void flush() {
        long end = System.currentTimeMillis() + 500;
        while (System.currentTimeMillis() <= end) {
            synchronized (taskQueueByTopic) {
                for (TraceDataSegment taskInfo : taskQueueByTopic.values()) {
                    taskInfo.sendAllData();
                }
            }
            synchronized (traceContextQueue) {
                if (traceContextQueue.isEmpty() && appenderQueue.isEmpty()) {
                    break;
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                break;
            }
        }
        log.info("------end trace send " + traceContextQueue.size() + "   " + appenderQueue.size());
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        flush();
        this.traceExecutor.shutdown();
        if (isStarted.get()) {
            traceProducer.shutdown();
        }
        this.removeShutdownHook();
    }

    public void registerShutDownHook() {
        if (shutDownHook == null) {
            shutDownHook = new Thread(new Runnable() {

                private final boolean hasShutdown = false;

                @Override
                public void run() {
                    synchronized (this) {
                        if (!this.hasShutdown) {
                            flush();
                        }
                    }
                }
            }, "ShutdownHookMQTrace");
            Runtime.getRuntime().addShutdownHook(shutDownHook);
        }
    }

    public void removeShutdownHook() {
        if (shutDownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutDownHook);
            } catch (IllegalStateException ignored) {
            }
        }
    }

    class AsyncRunnable implements Runnable {
        private boolean stopped;

        @Override
        public void run() {
            while (!stopped) {
                synchronized (traceContextQueue) {
                    long endTime = System.currentTimeMillis() + pollingTimeMil;
                    while (System.currentTimeMillis() < endTime) {
                        try {
                            TraceContext traceContext = traceContextQueue.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                            if (traceContext != null && !traceContext.getTraceBeans().isEmpty()) {
                                String traceTopicName = this.getTraceTopicName(traceContext.getRegionId());
                                TraceDataSegment traceDataSegment = taskQueueByTopic.get(traceTopicName);
                                if (traceDataSegment == null) {
                                    traceDataSegment = new TraceDataSegment(traceTopicName, traceContext.getRegionId());
                                    taskQueueByTopic.put(traceTopicName, traceDataSegment);
                                }
                                TraceTransferBean traceTransferBean = TraceDataEncoder.encoderFromContextBean(traceContext);
                                traceDataSegment.addTraceTransferBean(traceTransferBean);
                            }
                        } catch (InterruptedException ignore) {
                            log.debug("traceContextQueue#poll exception");
                        }
                    }
                    sendDataByTimeThreshold();
                    if (AsyncTraceDispatcher.this.stopped) {
                        this.stopped = true;
                    }
                }
            }
        }

        private void sendDataByTimeThreshold() {
            long now = System.currentTimeMillis();
            for (TraceDataSegment taskInfo : taskQueueByTopic.values()) {
                if (now - taskInfo.firstBeanAddTime >= waitTimeThresholdMil) {
                    taskInfo.sendAllData();
                }
            }
        }

        private String getTraceTopicName(String regionId) {
            AccessChannel accessChannel = AsyncTraceDispatcher.this.getAccessChannel();
            if (AccessChannel.CLOUD == accessChannel) {
                return TraceConstants.TRACE_TOPIC_PREFIX + regionId;
            }
            return AsyncTraceDispatcher.this.getTraceTopicName();
        }
    }

    @Getter
    @Setter
    class TraceDataSegment {

        private long firstBeanAddTime;

        private int currentMsgSize;

        private int currentMsgKeySize;

        private final String traceTopicName;

        private final String regionId;

        private final List<TraceTransferBean> traceTransferBeanList = new ArrayList<>();

        TraceDataSegment(String traceTopicName, String regionId) {
            this.traceTopicName = traceTopicName;
            this.regionId = regionId;
        }

        public void addTraceTransferBean(TraceTransferBean traceTransferBean) {
            initFirstBeanAddTime();
            this.traceTransferBeanList.add(traceTransferBean);
            this.currentMsgSize += traceTransferBean.getTransData().length();
            this.currentMsgKeySize = traceTransferBean.getTransKey().stream()
                .reduce(currentMsgKeySize, (acc, x) -> acc + x.length(), Integer::sum);
            if (currentMsgSize >= traceProducer.getMaxMessageSize() - 10 * 1000 || currentMsgKeySize >= MAX_MSG_KEY_SIZE) {
                List<TraceTransferBean> dataToSend = new ArrayList<>(traceTransferBeanList);
                AsyncDataSendTask asyncDataSendTask = new AsyncDataSendTask(traceTopicName, dataToSend);
                traceExecutor.submit(asyncDataSendTask);
                this.clear();
            }
        }

        public void sendAllData() {
            if (this.traceTransferBeanList.isEmpty()) {
                return;
            }
            List<TraceTransferBean> dataToSend = new ArrayList<>(traceTransferBeanList);
            AsyncDataSendTask asyncDataSendTask = new AsyncDataSendTask(traceTopicName, dataToSend);
            traceExecutor.submit(asyncDataSendTask);
            this.clear();
        }

        private void initFirstBeanAddTime() {
            if (firstBeanAddTime == 0) {
                firstBeanAddTime = System.currentTimeMillis();
            }
        }

        private void clear() {
            this.firstBeanAddTime = 0;
            this.currentMsgSize = 0;
            this.currentMsgKeySize = 0;
            this.traceTransferBeanList.clear();
        }
    }

    class AsyncDataSendTask implements Runnable {

        private final String traceTopicName;

        private final List<TraceTransferBean> traceTransferBeanList;

        public AsyncDataSendTask(String traceTopicName, List<TraceTransferBean> traceTransferBeanList) {
            this.traceTopicName = traceTopicName;
            this.traceTransferBeanList = traceTransferBeanList;
        }

        @Override
        public void run() {
            StringBuilder buffer = new StringBuilder(1024);
            Set<String> keySet = new HashSet<>();
            for (TraceTransferBean bean : traceTransferBeanList) {
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
            }
            sendTraceDataByMQ(keySet, buffer.toString(), traceTopicName);
        }

        private void sendTraceDataByMQ(Set<String> keySet, final String data, String traceTopic) {
            final Message message = new Message(traceTopic, data.getBytes(StandardCharsets.UTF_8));
            message.setKeys(keySet);
            try {
                Set<String> traceBrokerSet = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), traceTopic);
                SendCallback callback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {

                    }

                    @Override
                    public void onException(Throwable e) {
                        log.error("send trace data failed, the traceData is {}", data, e);
                    }
                };
                if (traceBrokerSet.isEmpty()) {
                    traceProducer.send(message, callback, 5000);
                } else {
                    traceProducer.send(message, (mqs, msg, arg) -> {
                        Set<String> brokerSet = (Set<String>) arg;
                        List<MessageQueue> filterMqs = new ArrayList<>();
                        for (MessageQueue queue : mqs) {
                            if (brokerSet.contains(queue.getBrokerName())) {
                                filterMqs.add(queue);
                            }
                        }
                        int index = sendWhichQueue.incrementAndGet();
                        int pos = index % filterMqs.size();
                        return filterMqs.get(pos);
                    }, traceBrokerSet, callback);
                }
            } catch (Exception e) {
                log.error("send trace data failed, the traceData is {}", data, e);
            }
        }

        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
            Set<String> brokerSet = new HashSet<>();
            TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                producer.getTopicPublishInfoTable().putIfAbsent(topic, new TopicPublishInfo());
                producer.getMqClientFactory().updateTopicRouteInfoFromNameServer(topic);
                topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            }
            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
                for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
                    brokerSet.add(queue.getBrokerName());
                }
            }
            return brokerSet;
        }
    }

}
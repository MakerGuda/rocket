package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Setter
public class ProduceAccumulator {

    private long totalHoldSize = 32 * 1024 * 1024;

    private long holdSize = 32 * 1024;

    private int holdMs = 10;

    private final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);

    private final GuardForSyncSendService guardThreadForSyncSend;

    private final GuardForAsyncSendService guardThreadForAsyncSend;

    private Map<AggregateKey, MessageAccumulation> syncSendBatchs = new ConcurrentHashMap<>();

    private Map<AggregateKey, MessageAccumulation> asyncSendBatchs = new ConcurrentHashMap<>();

    private final AtomicLong currentlyHoldSize = new AtomicLong(0);

    private final String instanceName;

    public ProduceAccumulator(String instanceName) {
        this.instanceName = instanceName;
        this.guardThreadForSyncSend = new GuardForSyncSendService(this.instanceName);
        this.guardThreadForAsyncSend = new GuardForAsyncSendService(this.instanceName);
    }

    private class GuardForSyncSendService extends ServiceThread {

        private final String serviceName;

        public GuardForSyncSendService(String clientInstanceName) {
            serviceName = String.format("Client_%s_GuardForSyncSend", clientInstanceName);
        }

        @Override
        public String getServiceName() {
            return serviceName;
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    this.doWork();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        private void doWork() throws InterruptedException {
            Collection<MessageAccumulation> values = syncSendBatchs.values();
            final int sleepTime = Math.max(1, holdMs / 2);
            for (MessageAccumulation v : values) {
                v.wakeup();
                synchronized (v) {
                    synchronized (v.closed) {
                        if (v.messagesSize.get() == 0) {
                            v.closed.set(true);
                            syncSendBatchs.remove(v.aggregateKey, v);
                        } else {
                            v.notify();
                        }
                    }
                }
            }
            Thread.sleep(sleepTime);
        }
    }

    private class GuardForAsyncSendService extends ServiceThread {

        private final String serviceName;

        public GuardForAsyncSendService(String clientInstanceName) {
            serviceName = String.format("Client_%s_GuardForAsyncSend", clientInstanceName);
        }

        @Override public String getServiceName() {
            return serviceName;
        }

        @Override public void run() {

            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.doWork();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        private void doWork() throws Exception {
            Collection<MessageAccumulation> values = asyncSendBatchs.values();
            final int sleepTime = Math.max(1, holdMs / 2);
            for (MessageAccumulation v : values) {
                if (v.readyToSend()) {
                    v.send();
                }
                synchronized (v.closed) {
                    if (v.messagesSize.get() == 0) {
                        v.closed.set(true);
                        asyncSendBatchs.remove(v.aggregateKey, v);
                    }
                }
            }
            Thread.sleep(sleepTime);
        }
    }

    void start() {
        guardThreadForSyncSend.start();
        guardThreadForAsyncSend.start();
    }

    void shutdown() {
        guardThreadForSyncSend.shutdown();
        guardThreadForAsyncSend.shutdown();
    }

    int getBatchMaxDelayMs() {
        return holdMs;
    }

    void batchMaxDelayMs(int holdMs) {
        if (holdMs <= 0 || holdMs > 30 * 1000) {
            throw new IllegalArgumentException(String.format("batchMaxDelayMs expect between 1ms and 30s, but get %d!", holdMs));
        }
        this.holdMs = holdMs;
    }

    long getBatchMaxBytes() {
        return holdSize;
    }

    void batchMaxBytes(long holdSize) {
        if (holdSize <= 0 || holdSize > 2 * 1024 * 1024) {
            throw new IllegalArgumentException(String.format("batchMaxBytes expect between 1B and 2MB, but get %d!", holdSize));
        }
        this.holdSize = holdSize;
    }

    long getTotalBatchMaxBytes() {
        return holdSize;
    }

    void totalBatchMaxBytes(long totalHoldSize) {
        if (totalHoldSize <= 0) {
            throw new IllegalArgumentException(String.format("totalBatchMaxBytes must bigger then 0, but get %d!", totalHoldSize));
        }
        this.totalHoldSize = totalHoldSize;
    }

    private MessageAccumulation getOrCreateSyncSendBatch(AggregateKey aggregateKey, DefaultMQProducer defaultMQProducer) {
        MessageAccumulation batch = syncSendBatchs.get(aggregateKey);
        if (batch != null) {
            return batch;
        }
        batch = new MessageAccumulation(aggregateKey, defaultMQProducer);
        MessageAccumulation previous = syncSendBatchs.putIfAbsent(aggregateKey, batch);
        return previous == null ? batch : previous;
    }

    private MessageAccumulation getOrCreateAsyncSendBatch(AggregateKey aggregateKey,
        DefaultMQProducer defaultMQProducer) {
        MessageAccumulation batch = asyncSendBatchs.get(aggregateKey);
        if (batch != null) {
            return batch;
        }
        batch = new MessageAccumulation(aggregateKey, defaultMQProducer);
        MessageAccumulation previous = asyncSendBatchs.putIfAbsent(aggregateKey, batch);
        return previous == null ? batch : previous;
    }

    SendResult send(Message msg, MessageQueue mq, DefaultMQProducer defaultMQProducer) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        AggregateKey partitionKey = new AggregateKey(msg, mq);
        while (true) {
            MessageAccumulation batch = getOrCreateSyncSendBatch(partitionKey, defaultMQProducer);
            int index = batch.add(msg);
            if (index == -1) {
                syncSendBatchs.remove(partitionKey, batch);
            } else {
                return batch.sendResults[index];
            }
        }
    }

    void send(Message msg, MessageQueue mq, SendCallback sendCallback, DefaultMQProducer defaultMQProducer) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        AggregateKey partitionKey = new AggregateKey(msg, mq);
        while (true) {
            MessageAccumulation batch = getOrCreateAsyncSendBatch(partitionKey, defaultMQProducer);
            if (!batch.add(msg, sendCallback)) {
                asyncSendBatchs.remove(partitionKey, batch);
            } else {
                return;
            }
        }
    }

    boolean tryAddMessage(Message message) {
        synchronized (currentlyHoldSize) {
            if (currentlyHoldSize.get() < totalHoldSize) {
                currentlyHoldSize.addAndGet(message.getBody().length);
                return true;
            } else {
                return false;
            }
        }
    }

    @Getter
    @Setter
    private static class AggregateKey {

        public String topic;

        public MessageQueue mq;

        public boolean waitStoreMsgOK;

        public String tag;

        public AggregateKey(Message message, MessageQueue mq) {
            this(message.getTopic(), mq, message.isWaitStoreMsgOK(), message.getTags());
        }

        public AggregateKey(String topic, MessageQueue mq, boolean waitStoreMsgOK, String tag) {
            this.topic = topic;
            this.mq = mq;
            this.waitStoreMsgOK = waitStoreMsgOK;
            this.tag = tag;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AggregateKey key = (AggregateKey) o;
            return waitStoreMsgOK == key.waitStoreMsgOK && topic.equals(key.topic) && Objects.equals(mq, key.mq) && Objects.equals(tag, key.tag);
        }

        @Override public int hashCode() {
            return Objects.hash(topic, mq, waitStoreMsgOK, tag);
        }
    }

    @Getter
    @Setter
    private class MessageAccumulation {

        private final DefaultMQProducer defaultMQProducer;

        private LinkedList<Message> messages;

        private LinkedList<SendCallback> sendCallbacks;

        private Set<String> keys;

        private final AtomicBoolean closed;

        private SendResult[] sendResults;

        private AggregateKey aggregateKey;

        private AtomicInteger messagesSize;

        private int count;

        private long createTime;

        public MessageAccumulation(AggregateKey aggregateKey, DefaultMQProducer defaultMQProducer) {
            this.defaultMQProducer = defaultMQProducer;
            this.messages = new LinkedList<>();
            this.sendCallbacks = new LinkedList<>();
            this.keys = new HashSet<>();
            this.closed = new AtomicBoolean(false);
            this.messagesSize = new AtomicInteger(0);
            this.aggregateKey = aggregateKey;
            this.count = 0;
            this.createTime = System.currentTimeMillis();
        }

        private boolean readyToSend() {
            return this.messagesSize.get() > holdSize || System.currentTimeMillis() >= this.createTime + holdMs;
        }

        public int add(
            Message msg) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
            int ret = -1;
            synchronized (this.closed) {
                if (this.closed.get()) {
                    return ret;
                }
                ret = this.count++;
                this.messages.add(msg);
                messagesSize.addAndGet(msg.getBody().length);
                String msgKeys = msg.getKeys();
                if (msgKeys != null) {
                    this.keys.addAll(Arrays.asList(msgKeys.split(MessageConst.KEY_SEPARATOR)));
                }
            }
            synchronized (this) {
                while (!this.closed.get()) {
                    if (readyToSend()) {
                        this.send();
                        break;
                    } else {
                        this.wait();
                    }
                }
                return ret;
            }
        }

        public boolean add(Message msg, SendCallback sendCallback) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
            synchronized (this.closed) {
                if (this.closed.get()) {
                    return false;
                }
                this.count++;
                this.messages.add(msg);
                this.sendCallbacks.add(sendCallback);
                messagesSize.getAndAdd(msg.getBody().length);
            }
            if (readyToSend()) {
                this.send();
            }
            return true;

        }

        public synchronized void wakeup() {
            if (this.closed.get()) {
                return;
            }
            this.notify();
        }

        private MessageBatch batch() {
            MessageBatch messageBatch = new MessageBatch(this.messages);
            messageBatch.setTopic(this.aggregateKey.topic);
            messageBatch.setWaitStoreMsgOK(this.aggregateKey.waitStoreMsgOK);
            messageBatch.setKeys(this.keys);
            messageBatch.setTags(this.aggregateKey.tag);
            MessageClientIDSetter.setUniqID(messageBatch);
            messageBatch.setBody(MessageDecoder.encodeMessages(this.messages));
            return messageBatch;
        }

        private void splitSendResults(SendResult sendResult) {
            if (sendResult == null) {
                throw new IllegalArgumentException("sendResult is null");
            }
            boolean isBatchConsumerQueue = !sendResult.getMsgId().contains(",");
            this.sendResults = new SendResult[this.count];
            if (!isBatchConsumerQueue) {
                String[] msgIds = sendResult.getMsgId().split(",");
                String[] offsetMsgIds = sendResult.getOffsetMsgId().split(",");
                if (offsetMsgIds.length != this.count || msgIds.length != this.count) {
                    throw new IllegalArgumentException("sendResult is illegal");
                }
                for (int i = 0; i < this.count; i++) {
                    this.sendResults[i] = new SendResult(sendResult.getSendStatus(), msgIds[i], sendResult.getMessageQueue(), sendResult.getQueueOffset() + i, sendResult.getTransactionId(), offsetMsgIds[i], sendResult.getRegionId());
                }
            } else {
                for (int i = 0; i < this.count; i++) {
                    this.sendResults[i] = sendResult;
                }
            }
        }

        private void send() throws InterruptedException, MQClientException, MQBrokerException, RemotingException {
            synchronized (this.closed) {
                if (this.closed.getAndSet(true)) {
                    return;
                }
            }
            MessageBatch messageBatch = this.batch();
            SendResult sendResult;
            try {
                if (defaultMQProducer != null) {
                    sendResult = defaultMQProducer.sendDirect(messageBatch, aggregateKey.mq, null);
                    this.splitSendResults(sendResult);
                } else {
                    throw new IllegalArgumentException("defaultMQProducer is null, can not send message");
                }
            } finally {
                currentlyHoldSize.addAndGet(-messagesSize.get());
                this.notifyAll();
            }
        }
    }

}
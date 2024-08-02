package org.apache.rocketmq.client.impl.factory;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.*;
import org.apache.rocketmq.client.impl.consumer.*;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.HeartbeatV2Result;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.*;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.rocketmq.remoting.rpc.ClientMetadata.topicRouteData2EndpointsForStaticTopic;

@Getter
@Setter
public class MQClientInstance {

    private final static long LOCK_TIMEOUT_MILLIS = 3000;

    private final static Logger log = LoggerFactory.getLogger(MQClientInstance.class);

    private final ClientConfig clientConfig;

    private final String clientId;

    /**
     * 启动时间
     */
    private final long bootTimestamp = System.currentTimeMillis();

    /**
     * 当前客户端，生产者组和生产者的映射 key: producerGroup
     */
    private final ConcurrentMap<String, MQProducerInner> producerTable = new ConcurrentHashMap<>();

    /**
     * 当前客户端，消费者组和消费者的映射 key: consumerGroup
     */
    private final ConcurrentMap<String, MQConsumerInner> consumerTable = new ConcurrentHashMap<>();

    /**
     * key: adminExtGroup
     */
    private final ConcurrentMap<String, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<>();

    private final NettyClientConfig nettyClientConfig;

    private final MQClientAPIImpl mQClientAPIImpl;

    private final MQAdminImpl mQAdminImpl;

    /**
     * key: topic
     */
    private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    /**
     * key: topic value:{ key: mq  value: brokerName}
     */
    private final ConcurrentMap<String, ConcurrentMap<MessageQueue, String>> topicEndPointsTable = new ConcurrentHashMap<>();

    private final Lock lockNamesrv = new ReentrantLock();

    private final Lock lockHeartbeat = new ReentrantLock();

    /**
     * key: brokerName  value:{ key: brokerId  value: brokerAddr}
     */
    private final ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();

    /**
     * key: brokerName value: brokerAddr
     */
    private final ConcurrentMap<String, HashMap<String, Integer>> brokerVersionTable = new ConcurrentHashMap<>();

    /**
     * broker address set
     */
    private final Set<String> brokerSupportV2HeartbeatSet = new HashSet<>();

    private final ConcurrentMap<String, Integer> brokerAddrHeartbeatFingerprintTable = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));

    private final ScheduledExecutorService fetchRemoteConfigExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryFetchRemoteConfigScheduledThread"));

    /**
     * 消息拉取服务
     */
    private final PullMessageService pullMessageService;

    /**
     * 重平衡服务
     */
    private final RebalanceService rebalanceService;

    private final DefaultMQProducer defaultMQProducer;

    private final ConsumerStatsManager consumerStatsManager;

    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
    private final Random random = new Random();
    /**
     * 初始状态
     */
    private ServiceState serviceState = ServiceState.CREATE_JUST;

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
        this.nettyClientConfig.setSocksProxyConfig(clientConfig.getSocksProxyConfig());
        this.nettyClientConfig.setScanAvailableNameSrv(false);
        //客户端远程处理器
        ClientRemotingProcessor clientRemotingProcessor = new ClientRemotingProcessor(this);
        ChannelEventListener channelEventListener;
        if (clientConfig.isEnableHeartbeatChannelEventListener()) {
            channelEventListener = new ChannelEventListener() {
                private final ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = MQClientInstance.this.brokerAddrTable;

                @Override
                public void onChannelConnect(String remoteAddr, Channel channel) {
                }

                @Override
                public void onChannelClose(String remoteAddr, Channel channel) {
                }

                @Override
                public void onChannelException(String remoteAddr, Channel channel) {
                }

                @Override
                public void onChannelIdle(String remoteAddr, Channel channel) {
                }

                @Override
                public void onChannelActive(String remoteAddr, Channel channel) {
                    for (Map.Entry<String, HashMap<Long, String>> addressEntry : brokerAddrTable.entrySet()) {
                        for (Map.Entry<Long, String> entry : addressEntry.getValue().entrySet()) {
                            String addr = entry.getValue();
                            if (addr.equals(remoteAddr)) {
                                long id = entry.getKey();
                                String brokerName = addressEntry.getKey();
                                //客户端往broker发送心跳检测成功，重平衡消费者
                                if (sendHeartbeatToBroker(id, brokerName, addr)) {
                                    rebalanceImmediately();
                                }
                                break;
                            }
                        }
                    }
                }
            };
        } else {
            channelEventListener = null;
        }
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, clientRemotingProcessor, rpcHook, clientConfig, channelEventListener);
        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }
        this.clientId = clientId;
        this.mQAdminImpl = new MQAdminImpl(this);
        this.pullMessageService = new PullMessageService(this);
        this.rebalanceService = new RebalanceService(this);
        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);
        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);
        log.info("Created a new client Instance, InstanceIndex:{}, ClientID:{}, ClientConfig:{}, ClientVersion:{}, SerializerType:{}", instanceIndex, this.clientId, this.clientConfig, MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    /**
     * 构建主题发布信息
     */
    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);
        if (route.getOrderTopicConf() != null && !route.getOrderTopicConf().isEmpty()) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }
            info.setOrderTopic(true);
        } else if (route.getOrderTopicConf() == null && route.getTopicQueueMappingByBroker() != null && !route.getTopicQueueMappingByBroker().isEmpty()) {
            info.setOrderTopic(false);
            ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, route);
            info.getMessageQueueList().addAll(mqEndPoints.keySet());
            info.getMessageQueueList().sort((mq1, mq2) -> MixAll.compareInteger(mq1.getQueueId(), mq2.getQueueId()));
        } else {
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds);
            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }
                    if (null == brokerData) {
                        continue;
                    }
                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }
            info.setOrderTopic(false);
        }
        return info;
    }

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<>();
        if (route.getTopicQueueMappingByBroker() != null
                && !route.getTopicQueueMappingByBroker().isEmpty()) {
            ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, route);
            return mqEndPoints.keySet();
        }
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }
        return mqList;
    }

    /**
     * 启动mq客户端工厂
     */
    public void start() throws MQClientException {
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        //获取namesrv地址
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    //启动请求-响应通道
                    this.mQClientAPIImpl.start();

                    //启动定时任务
                    this.startScheduledTask();

                    // Start pull service
                    this.pullMessageService.start();
                    // Start rebalance service
                    this.rebalanceService.start();
                    // Start push service
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    /**
     * 启动一系列的定时任务
     */
    private void startScheduledTask() {
        //定时更新namesrv地址
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                } catch (Throwable t) {
                    log.error("ScheduledTask fetchNameServerAddr exception", t);
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        //定时从namesrv拉取并更新主题路由信息
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.updateTopicRouteInfoFromNameServer();
            } catch (Throwable t) {
                log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", t);
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

        //定时清除下线broker
        //定时往所有broker发送心跳检测
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.cleanOfflineBroker();
                MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
            } catch (Throwable t) {
                log.error("ScheduledTask sendHeartbeatToAllBroker exception", t);
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        //持久化消费者偏移量
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.persistAllConsumerOffset();
            } catch (Throwable t) {
                log.error("ScheduledTask persistAllConsumerOffset exception", t);
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        //调整线程池参数
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.adjustThreadPool();
            } catch (Throwable t) {
                log.error("ScheduledTask adjustThreadPool exception", t);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * 从namesrv拉取并更新主题路由信息
     */
    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<>();
        // Consumer
        {
            for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    //获取消费者的订阅关系
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }
        // Producer
        {
            for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }
        //此时已经获取所有生产者和消费者相关的主题，从namesrv拉取并更新主题路由信息
        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * 移除下线的broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<>(this.brokerAddrTable.size(), 1);
                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<>(oneTable.size(), 1);
                        cloneAddrTable.putAll(oneTable);

                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }
                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    public void checkClientInBroker() throws MQClientException {
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }
            for (SubscriptionData subscriptionData : subscriptionInner) {
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());
                if (addr != null) {
                    try {
                        this.getMQClientAPIImpl().checkClientInBroker(addr, entry.getKey(), this.clientId, subscriptionData, clientConfig.getMqClientApiTimeout());
                    } catch (Exception e) {
                        if (e instanceof MQClientException) {
                            throw (MQClientException) e;
                        } else {
                            throw new MQClientException("Check client in broker error, maybe because you use " + subscriptionData.getExpressionType() + " to filter message, but server has not been upgraded to support!" + "This error would not affect the launch of consumer, but may has impact on message receiving if you " + "have use the new features which are not supported by server, please check the log!", e);
                        }
                    }
                }
            }
        }
    }

    public void sendHeartbeatToAllBrokerWithLockV2(boolean isRebalance) {
        if (this.lockHeartbeat.tryLock()) {
            try {
                if (clientConfig.isUseHeartbeatV2()) {
                    this.sendHeartbeatToAllBrokerV2(isRebalance);
                } else {
                    this.sendHeartbeatToAllBroker();
                }
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBrokerWithLockV2 exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("sendHeartbeatToAllBrokerWithLockV2 lock heartBeat, but failed.");
        }
    }

    /**
     * 往所有broker发送心跳检测
     */
    public boolean sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                if (clientConfig.isUseHeartbeatV2()) {
                    return this.sendHeartbeatToAllBrokerV2(false);
                } else {
                    return this.sendHeartbeatToAllBroker();
                }
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed. [{}]", this.clientId);
        }
        return false;
    }

    /**
     * 持久化消费者偏移量
     */
    private void persistAllConsumerOffset() {
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            impl.persistConsumerOffset();
        }
    }

    /**
     * 调整线程池参数
     */
    public void adjustThreadPool() {
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * 更新主题路由信息
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    /**
     * 判断brokerAddr是否在主题路由信息表中
     */
    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        for (Entry<String, TopicRouteData> entry : this.topicRouteTable.entrySet()) {
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * 发送心跳检测
     *
     * @param id         brokerId
     * @param brokerName brokerName
     * @param addr       brokerAddr
     */
    public boolean sendHeartbeatToBroker(long id, String brokerName, String addr) {
        if (this.lockHeartbeat.tryLock()) {
            final HeartbeatData heartbeatDataWithSub = this.prepareHeartbeatData(false);
            final boolean producerEmpty = heartbeatDataWithSub.getProducerDataSet().isEmpty();
            final boolean consumerEmpty = heartbeatDataWithSub.getConsumerDataSet().isEmpty();
            if (producerEmpty && consumerEmpty) {
                log.warn("sendHeartbeatToBroker sending heartbeat, but no consumer and no producer. [{}]", this.clientId);
                return false;
            }
            try {
                if (clientConfig.isUseHeartbeatV2()) {
                    int currentHeartbeatFingerprint = heartbeatDataWithSub.computeHeartbeatFingerprint();
                    heartbeatDataWithSub.setHeartbeatFingerprint(currentHeartbeatFingerprint);
                    HeartbeatData heartbeatDataWithoutSub = this.prepareHeartbeatData(true);
                    heartbeatDataWithoutSub.setHeartbeatFingerprint(currentHeartbeatFingerprint);
                    return this.sendHeartbeatToBrokerV2(id, brokerName, addr, heartbeatDataWithSub, heartbeatDataWithoutSub, currentHeartbeatFingerprint);
                } else {
                    return this.sendHeartbeatToBroker(id, brokerName, addr, heartbeatDataWithSub);
                }
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed. [{}]", this.clientId);
        }
        return false;
    }

    /**
     * 往指定broker发送心跳检测
     */
    private boolean sendHeartbeatToBroker(long id, String brokerName, String addr, HeartbeatData heartbeatData) {
        try {
            int version = this.mQClientAPIImpl.sendHeartbeat(addr, heartbeatData, clientConfig.getMqClientApiTimeout());
            if (!this.brokerVersionTable.containsKey(brokerName)) {
                this.brokerVersionTable.put(brokerName, new HashMap<>(4));
            }
            this.brokerVersionTable.get(brokerName).put(addr, version);
            long times = this.sendHeartbeatTimesTotal.getAndIncrement();
            if (times % 20 == 0) {
                log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                log.info(heartbeatData.toString());
            }
            return true;
        } catch (Exception e) {
            if (this.isBrokerInNameServer(addr)) {
                log.warn("send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
            } else {
                log.warn("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName, id, addr, e);
            }
        }
        return false;
    }

    /**
     * 往所有broker发送心跳检测
     */
    private boolean sendHeartbeatToAllBroker() {
        final HeartbeatData heartbeatData = this.prepareHeartbeatData(false);
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer. [{}]", this.clientId);
            return false;
        }
        if (this.brokerAddrTable.isEmpty()) {
            return false;
        }
        for (Entry<String, HashMap<Long, String>> brokerClusterInfo : this.brokerAddrTable.entrySet()) {
            String brokerName = brokerClusterInfo.getKey();
            HashMap<Long, String> oneTable = brokerClusterInfo.getValue();
            if (oneTable == null) {
                continue;
            }
            for (Entry<Long, String> singleBrokerInstance : oneTable.entrySet()) {
                Long id = singleBrokerInstance.getKey();
                String addr = singleBrokerInstance.getValue();
                if (addr == null) {
                    continue;
                }
                if (consumerEmpty && MixAll.MASTER_ID != id) {
                    continue;
                }
                sendHeartbeatToBroker(id, brokerName, addr, heartbeatData);
            }
        }
        return true;
    }

    /**
     * 客户端往broker发送心跳检测
     */
    private boolean sendHeartbeatToBrokerV2(long id, String brokerName, String addr, HeartbeatData heartbeatDataWithSub, HeartbeatData heartbeatDataWithoutSub, int currentHeartbeatFingerprint) {
        try {
            int version;
            boolean isBrokerSupportV2 = brokerSupportV2HeartbeatSet.contains(addr);
            HeartbeatV2Result heartbeatV2Result;
            if (isBrokerSupportV2 && null != brokerAddrHeartbeatFingerprintTable.get(addr) && brokerAddrHeartbeatFingerprintTable.get(addr) == currentHeartbeatFingerprint) {
                heartbeatV2Result = this.mQClientAPIImpl.sendHeartbeatV2(addr, heartbeatDataWithoutSub, clientConfig.getMqClientApiTimeout());
                if (heartbeatV2Result.isSubChange()) {
                    brokerAddrHeartbeatFingerprintTable.remove(addr);
                }
                log.info("sendHeartbeatToAllBrokerV2 simple brokerName: {} subChange: {} brokerAddrHeartbeatFingerprintTable: {}", brokerName, heartbeatV2Result.isSubChange(), JSON.toJSONString(brokerAddrHeartbeatFingerprintTable));
            } else {
                heartbeatV2Result = this.mQClientAPIImpl.sendHeartbeatV2(addr, heartbeatDataWithSub, clientConfig.getMqClientApiTimeout());
                if (heartbeatV2Result.isSupportV2()) {
                    brokerSupportV2HeartbeatSet.add(addr);
                    if (heartbeatV2Result.isSubChange()) {
                        brokerAddrHeartbeatFingerprintTable.remove(addr);
                    } else if (!brokerAddrHeartbeatFingerprintTable.containsKey(addr) || brokerAddrHeartbeatFingerprintTable.get(addr) != currentHeartbeatFingerprint) {
                        brokerAddrHeartbeatFingerprintTable.put(addr, currentHeartbeatFingerprint);
                    }
                }
                log.info("sendHeartbeatToAllBrokerV2 normal brokerName: {} subChange: {} brokerAddrHeartbeatFingerprintTable: {}", brokerName, heartbeatV2Result.isSubChange(), JSON.toJSONString(brokerAddrHeartbeatFingerprintTable));
            }
            version = heartbeatV2Result.getVersion();
            if (!this.brokerVersionTable.containsKey(brokerName)) {
                this.brokerVersionTable.put(brokerName, new HashMap<>(4));
            }
            this.brokerVersionTable.get(brokerName).put(addr, version);
            long times = this.sendHeartbeatTimesTotal.getAndIncrement();
            if (times % 20 == 0) {
                log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                log.info(heartbeatDataWithSub.toString());
            }
            return true;
        } catch (Exception e) {
            if (this.isBrokerInNameServer(addr)) {
                log.warn("sendHeartbeatToAllBrokerV2 send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
            } else {
                log.warn("sendHeartbeatToAllBrokerV2 send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName, id, addr, e);
            }
        }
        return false;
    }

    /**
     * 往所有broker发送心跳检测
     */
    private boolean sendHeartbeatToAllBrokerV2(boolean isRebalance) {
        final HeartbeatData heartbeatDataWithSub = this.prepareHeartbeatData(false);
        final boolean producerEmpty = heartbeatDataWithSub.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatDataWithSub.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sendHeartbeatToAllBrokerV2 sending heartbeat, but no consumer and no producer. [{}]", this.clientId);
            return false;
        }
        if (this.brokerAddrTable.isEmpty()) {
            return false;
        }
        if (isRebalance) {
            resetBrokerAddrHeartbeatFingerprintMap();
        }
        int currentHeartbeatFingerprint = heartbeatDataWithSub.computeHeartbeatFingerprint();
        heartbeatDataWithSub.setHeartbeatFingerprint(currentHeartbeatFingerprint);
        HeartbeatData heartbeatDataWithoutSub = this.prepareHeartbeatData(true);
        heartbeatDataWithoutSub.setHeartbeatFingerprint(currentHeartbeatFingerprint);
        for (Entry<String, HashMap<Long, String>> brokerClusterInfo : this.brokerAddrTable.entrySet()) {
            String brokerName = brokerClusterInfo.getKey();
            HashMap<Long, String> oneTable = brokerClusterInfo.getValue();
            if (oneTable == null) {
                continue;
            }
            for (Entry<Long, String> singleBrokerInstance : oneTable.entrySet()) {
                Long id = singleBrokerInstance.getKey();
                String addr = singleBrokerInstance.getValue();
                if (addr == null) {
                    continue;
                }
                if (consumerEmpty && MixAll.MASTER_ID != id) {
                    continue;
                }
                sendHeartbeatToBrokerV2(id, brokerName, addr, heartbeatDataWithSub, heartbeatDataWithoutSub, currentHeartbeatFingerprint);
            }
        }
        return true;
    }

    /**
     * 拉取并更新主题路由信息
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault, DefaultMQProducer defaultMQProducer) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    if (isDefault && defaultMQProducer != null) {
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(clientConfig.getMqClientApiTimeout());
                        if (topicRouteData != null) {
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, clientConfig.getMqClientApiTimeout());
                    }
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        boolean changed = topicRouteData.topicRouteDataChanged(old);
                        if (!changed) {
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        if (changed) {
                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }
                            // Update endpoint map
                            {
                                ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, topicRouteData);
                                if (!mqEndPoints.isEmpty()) {
                                    topicEndPointsTable.put(topic, mqEndPoints);
                                }
                            }

                            //更新生产者主题发布信息
                            {
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);
                                for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            //更新消费者主题订阅信息
                            if (!consumerTable.isEmpty()) {
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            TopicRouteData cloneTopicRouteData = new TopicRouteData(topicRouteData);
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}. [{}]", topic, this.clientId);
                    }
                } catch (MQClientException e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } catch (RemotingException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                    throw new IllegalStateException(e);
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms. [{}]", LOCK_TIMEOUT_MILLIS, this.clientId);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }
        return false;
    }

    /**
     * 准备心跳检测数据
     */
    private HeartbeatData prepareHeartbeatData(boolean isWithoutSub) {
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID(this.clientId);

        //消费者
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());
                if (!isWithoutSub) {
                    consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                }
                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        //生产者
        for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());
                heartbeatData.getProducerDataSet().add(producerData);
            }
        }
        heartbeatData.setWithoutSub(isWithoutSub);
        return heartbeatData;
    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        for (Entry<String, TopicRouteData> itNext : this.topicRouteTable.entrySet()) {
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }

        return false;
    }

    /**
     * 判断当前主题路由信息是否需要更新
     */
    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        Iterator<Entry<String, MQProducerInner>> producerIterator = this.producerTable.entrySet().iterator();
        while (producerIterator.hasNext() && !result) {
            Entry<String, MQProducerInner> entry = producerIterator.next();
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                result = impl.isPublishTopicNeedUpdate(topic);
            }
        }
        if (result) {
            return true;
        }
        Iterator<Entry<String, MQConsumerInner>> consumerIterator = this.consumerTable.entrySet().iterator();
        while (consumerIterator.hasNext() && !result) {
            Entry<String, MQConsumerInner> entry = consumerIterator.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                result = impl.isSubscribeTopicNeedUpdate(topic);
            }
        }
        return result;
    }

    /**
     * MQClientInstance关闭
     */
    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        // AdminExt
        if (!this.adminExtTable.isEmpty())
            return;

        // Producer
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            switch (this.serviceState) {
                case RUNNING:
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);
                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case CREATE_JUST:
                case SHUTDOWN_ALREADY:
                default:
                    break;
            }
        }
    }

    /**
     * 注册消费者
     */
    public synchronized boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }
        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }
        return true;
    }

    /**
     * 注销消费者
     */
    public synchronized void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClient(null, group);
    }

    /**
     * 注销当前客户端
     */
    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        for (Entry<String, HashMap<Long, String>> brokerClusterInfo : this.brokerAddrTable.entrySet()) {
            String brokerName = brokerClusterInfo.getKey();
            HashMap<Long, String> oneTable = brokerClusterInfo.getValue();
            if (oneTable == null) {
                continue;
            }
            for (Entry<Long, String> singleBrokerInstance : oneTable.entrySet()) {
                String addr = singleBrokerInstance.getValue();
                if (addr != null) {
                    try {
                        this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, clientConfig.getMqClientApiTimeout());
                        log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, singleBrokerInstance.getKey(), addr);
                    } catch (RemotingException e) {
                        log.warn("unregister client RemotingException from broker: {}, {}", addr, e.getMessage());
                    } catch (InterruptedException e) {
                        log.warn("unregister client InterruptedException from broker: {}, {}", addr, e.getMessage());
                    } catch (MQBrokerException e) {
                        log.warn("unregister client MQBrokerException from broker: {}, {}", addr, e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * 注册生产者
     *
     * @param group    生产者组
     * @param producer 生产者实例
     * @return 是否注册成功
     */
    public synchronized boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }
        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }
        return true;
    }

    /**
     * 注销生产者
     */
    public synchronized void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClient(group, null);
    }

    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }
        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }
        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    public void rebalanceLater(long delayMillis) {
        if (delayMillis <= 0) {
            this.rebalanceService.wakeup();
        } else {
            this.scheduledExecutorService.schedule(MQClientInstance.this.rebalanceService::wakeup, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 消费者重平衡
     */
    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    /**
     * 消费者重平衡
     */
    public boolean doRebalance() {
        boolean balanced = true;
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (!impl.tryRebalance()) {
                        balanced = false;
                    }
                } catch (Throwable e) {
                    log.error("doRebalance for consumer group [{}] exception", entry.getKey(), e);
                }
            }
        }

        return balanced;
    }

    /**
     * 获取生产者组里的生产者
     */
    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    /**
     * 获取消费者组例的消费者
     */
    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    /**
     * 返回队列的broker名称
     */
    public String getBrokerNameFromMessageQueue(final MessageQueue mq) {
        if (topicEndPointsTable.get(mq.getTopic()) != null && !topicEndPointsTable.get(mq.getTopic()).isEmpty()) {
            return topicEndPointsTable.get(mq.getTopic()).get(mq);
        }
        return mq.getBrokerName();
    }

    /**
     * 获取brokerName对应的master地址
     */
    public String findBrokerAddressInPublish(final String brokerName) {
        if (brokerName == null) {
            return null;
        }
        HashMap<Long, String> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }
        return null;
    }

    public FindBrokerResult findBrokerAddressInSubscribe(final String brokerName, final long brokerId, final boolean onlyThisBroker) {
        if (brokerName == null) {
            return null;
        }
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;
        HashMap<Long, String> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;
            if (!found && slave) {
                brokerAddr = map.get(brokerId + 1);
                found = brokerAddr != null;
            }

            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = brokerAddr != null;
            }
        }
        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }
        return null;
    }

    private int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        return 0;
    }

    public List<String> findConsumerIdList(final String topic, final String group) {
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }
        if (null != brokerAddr) {
            try {
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, clientConfig.getMqClientApiTimeout());
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }
        return null;
    }

    public Set<MessageQueueAssignment> queryAssignment(final String topic, final String consumerGroup, final String strategyName, final MessageModel messageModel, int timeout) throws RemotingException, InterruptedException, MQBrokerException {
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }
        if (null != brokerAddr) {
            return this.mQClientAPIImpl.queryAssignment(brokerAddr, topic, consumerGroup, clientId, strategyName, messageModel, timeout);
        }
        return null;
    }

    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                BrokerData bd = brokers.get(random.nextInt(brokers.size()));
                return bd.selectBrokerAddr();
            }
        }
        return null;
    }

    public synchronized void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            consumer.suspend();
            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException ignored) {
            }
            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        consumer.updateConsumeOffset(mq, offset);
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        iterator.remove();
                    } catch (Exception e) {
                        log.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.resume();
            }
        }
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return new HashMap<>();
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg,
                                                               final String consumerGroup,
                                                               final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (mqConsumerInner instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;
            return consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
        }
        return null;
    }

    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (mqConsumerInner == null) {
            return null;
        }
        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, null);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION, MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
        return consumerRunningInfo;
    }

    private void resetBrokerAddrHeartbeatFingerprintMap() {
        brokerAddrHeartbeatFingerprintTable.clear();
    }

}
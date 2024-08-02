package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.NetworkUtil;

import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class BrokerConfig extends BrokerIdentity {

    private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();
    /**
     * broker配置文件路径
     */
    private String brokerConfigPath = null;
    /**
     * rocketmq根文件目录
     */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    /**
     * namesrv地址
     */
    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    /**
     * 监听端口号
     */
    @ImportantField
    private int listenPort = 6888;
    @ImportantField
    private String brokerIP1 = NetworkUtil.getLocalAddress();
    private String brokerIP2 = NetworkUtil.getLocalAddress();
    @ImportantField
    private boolean recoverConcurrently = false;
    /**
     * broker读写权限
     */
    private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;
    /**
     * 默认创建主题的队列数
     */
    private int defaultTopicQueueNums = 8;
    /**
     * 是否自动创建主题
     */
    @ImportantField
    private boolean autoCreateTopicEnable = true;
    private boolean clusterTopicEnable = true;
    private boolean brokerTopicEnable = true;
    @ImportantField
    private boolean autoCreateSubscriptionGroup = true;
    private String messageStorePlugIn = "";
    @ImportantField
    private String msgTraceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;

    @ImportantField
    private boolean traceTopicEnable = false;

    /**
     * 发送消息的线程数
     */
    private int sendMessageThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);

    private int putMessageFutureThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);

    private int pullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;

    private int litePullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;

    private int ackMessageThreadPoolNums = 16;

    private int processReplyMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;

    private int queryMessageThreadPoolNums = 8 + PROCESSOR_NUMBER;

    private int adminBrokerThreadPoolNums = 16;

    private int clientManageThreadPoolNums = 32;

    private int consumerManageThreadPoolNums = 32;

    private int loadBalanceProcessorThreadPoolNums = 32;

    private int heartbeatThreadPoolNums = Math.min(32, PROCESSOR_NUMBER);

    private int recoverThreadPoolNums = 32;

    private int endTransactionThreadPoolNums = Math.max(8 + PROCESSOR_NUMBER * 2, sendMessageThreadPoolNums * 4);

    private int flushConsumerOffsetInterval = 1000 * 5;

    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    @ImportantField
    private boolean rejectTransactionMessage = false;

    @ImportantField
    private boolean fetchNameSrvAddrByDnsLookup = false;

    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;

    private int sendThreadPoolQueueCapacity = 10000;

    private int putThreadPoolQueueCapacity = 10000;

    private int pullThreadPoolQueueCapacity = 100000;

    private int litePullThreadPoolQueueCapacity = 100000;

    private int ackThreadPoolQueueCapacity = 100000;

    private int replyThreadPoolQueueCapacity = 10000;

    private int queryThreadPoolQueueCapacity = 20000;

    private int clientManagerThreadPoolQueueCapacity = 1000000;

    private int consumerManagerThreadPoolQueueCapacity = 1000000;

    private int heartbeatThreadPoolQueueCapacity = 50000;

    private int endTransactionPoolQueueCapacity = 100000;

    private int adminBrokerThreadPoolQueueCapacity = 10000;

    private int loadBalanceThreadPoolQueueCapacity = 100000;

    private boolean longPollingEnable = true;

    private long shortPollingTimeMills = 1000;

    private boolean notifyConsumerIdsChangedEnable = true;

    private boolean highSpeedMode = false;

    private int commercialBaseCount = 1;

    private int commercialSizePerMsg = 4 * 1024;

    private boolean accountStatsEnable = true;

    private boolean accountStatsPrintZeroValues = true;

    private boolean transferMsgByHeap = true;

    private String regionId = MixAll.DEFAULT_TRACE_REGION_ID;

    private int registerBrokerTimeoutMills = 24000;

    private int sendHeartbeatTimeoutMillis = 1000;

    private boolean slaveReadEnable = false;

    private boolean disableConsumeIfConsumerReadSlowly = false;

    private long consumerFallbehindThreshold = 1024L * 1024 * 1024 * 16;

    private boolean brokerFastFailureEnable = true;

    private long waitTimeMillsInSendQueue = 200;

    private long waitTimeMillsInPullQueue = 5 * 1000;

    private long waitTimeMillsInLitePullQueue = 5 * 1000;

    private long waitTimeMillsInHeartbeatQueue = 31 * 1000;

    private long waitTimeMillsInTransactionQueue = 3 * 1000;

    private long waitTimeMillsInAckQueue = 3000;

    private long waitTimeMillsInAdminBrokerQueue = 5 * 1000;

    private long startAcceptSendRequestTimeStamp = 0L;

    private boolean traceOn = true;

    private boolean enableCalcFilterBitMap = false;

    private boolean rejectPullConsumerEnable = false;

    private int expectConsumerNumUseFilter = 32;

    private int maxErrorRateOfBloomFilter = 20;

    private long filterDataCleanTimeSpan = 24 * 3600 * 1000;

    private boolean filterSupportRetry = false;

    private boolean enablePropertyFilter = false;

    private boolean compressedRegister = false;

    private boolean forceRegister = true;

    private int registerNameServerPeriod = 1000 * 30;

    private int brokerHeartbeatInterval = 1000;

    private long brokerNotActiveTimeoutMillis = 10 * 1000;

    private boolean enableNetWorkFlowControl = false;

    private boolean enableBroadcastOffsetStore = true;

    private long broadcastOffsetExpireSecond = 2 * 60;

    private long broadcastOffsetExpireMaxSecond = 5 * 60;

    private int popPollingSize = 1024;

    private int popPollingMapSize = 100000;

    private long maxPopPollingSize = 100000;

    private int reviveQueueNum = 8;

    private long reviveInterval = 1000;

    private long reviveMaxSlow = 3;

    private long reviveScanTime = 10000;

    private boolean enableSkipLongAwaitingAck = false;

    private long reviveAckWaitMs = TimeUnit.MINUTES.toMillis(3);

    private boolean enablePopLog = false;

    private boolean enablePopBufferMerge = false;

    private int popCkStayBufferTime = 10 * 1000;

    private int popCkStayBufferTimeOut = 3 * 1000;

    private int popCkMaxBufferSize = 200000;

    private int popCkOffsetMaxQueueSize = 20000;

    private boolean enablePopBatchAck = false;

    private boolean enableNotifyAfterPopOrderLockRelease = true;

    private boolean initPopOffsetByCheckMsgInMem = true;

    private boolean retrieveMessageFromPopRetryTopicV1 = true;

    private boolean enableRetryTopicV2 = false;

    private boolean realTimeNotifyConsumerChange = true;

    private boolean litePullMessageEnable = true;

    private int syncBrokerMemberGroupPeriod = 1000;

    private long loadBalancePollNameServerInterval = 1000 * 30;

    private int cleanOfflineBrokerInterval = 1000 * 30;

    private boolean serverLoadBalancerEnable = true;

    private MessageRequestMode defaultMessageRequestMode = MessageRequestMode.PULL;

    private int defaultPopShareQueueNum = -1;

    @ImportantField
    private long transactionTimeOut = 6 * 1000;

    @ImportantField
    private int transactionCheckMax = 15;

    @ImportantField
    private long transactionCheckInterval = 30 * 1000;

    private long transactionMetricFlushInterval = 3 * 1000;

    private int transactionOpMsgMaxSize = 4096;

    private int transactionOpBatchInterval = 3000;

    @ImportantField
    private boolean aclEnable = false;

    private boolean storeReplyMessageEnable = true;

    private boolean enableDetailStat = true;

    private boolean autoDeleteUnusedStats = false;

    private boolean isolateLogEnable = false;

    private long forwardTimeout = 3 * 1000;

    private boolean enableSlaveActingMaster = false;

    private boolean enableRemoteEscape = false;

    private boolean skipPreOnline = false;

    private boolean asyncSendEnable = true;

    private boolean useServerSideResetOffset = true;

    private long consumerOffsetUpdateVersionStep = 500;

    private long delayOffsetUpdateVersionStep = 200;

    private boolean lockInStrictMode = false;

    private boolean compatibleWithOldNameSrv = true;

    private boolean enableControllerMode = false;

    private String controllerAddr = "";

    private boolean fetchControllerAddrByDnsLookup = false;

    private long syncBrokerMetadataPeriod = 5 * 1000;

    private long checkSyncStateSetPeriod = 5 * 1000;

    private long syncControllerMetadataPeriod = 10 * 1000;

    private long controllerHeartBeatTimeoutMills = 10 * 1000;

    private boolean validateSystemTopicWhenUpdateTopic = true;

    private int brokerElectionPriority = Integer.MAX_VALUE;

    private boolean useStaticSubscription = false;

    private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

    private int metricsOtelCardinalityLimit = 50 * 1000;

    private String metricsGrpcExporterTarget = "";

    private String metricsGrpcExporterHeader = "";

    private long metricGrpcExporterTimeOutInMills = 3 * 1000;

    private long metricGrpcExporterIntervalInMills = 60 * 1000;

    private long metricLoggingExporterIntervalInMills = 10 * 1000;

    private int metricsPromExporterPort = 5557;

    private String metricsPromExporterHost = "";

    private String metricsLabel = "";

    private boolean metricsInDelta = false;

    private long channelExpiredTimeout = 1000 * 120;

    private long subscriptionExpiredTimeout = 1000 * 60 * 10;

    private boolean estimateAccumulation = true;

    private boolean coldCtrStrategyEnable = false;

    private boolean usePIDColdCtrStrategy = true;

    private long cgColdReadThreshold = 3 * 1024 * 1024;

    private long globalColdReadThreshold = 100 * 1024 * 1024;

    private long fetchNamesrvAddrInterval = 10 * 1000;

    private boolean popResponseReturnActualRetryTopic = false;

    private boolean enableSingleTopicRegister = false;

    private boolean enableMixedMessageType = false;

    private boolean enableSplitRegistration = false;

    private long popInflightMessageThreshold = 10000;

    private boolean enablePopMessageThreshold = false;

    private int splitRegistrationSize = 800;

    private String configBlackList = "configBlackList;brokerConfigPath";

}
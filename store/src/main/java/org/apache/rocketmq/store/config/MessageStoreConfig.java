package org.apache.rocketmq.store.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.StoreType;
import org.apache.rocketmq.store.queue.BatchConsumeQueue;

import java.io.File;

@Getter
@Setter
public class MessageStoreConfig {

    public static final String MULTI_PATH_SPLITTER = System.getProperty("rocketmq.broker.multiPathSplitter", ",");

    @ImportantField
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    @ImportantField
    private String storePathCommitLog = null;

    @ImportantField
    private String storePathDLedgerCommitLog = null;

    @ImportantField
    private String storePathEpochFile = null;

    @ImportantField
    private String storePathBrokerIdentity = null;

    private String readOnlyCommitLogStorePaths = null;

    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    private int compactionMappedFileSize = 100 * 1024 * 1024;

    private int compactionCqMappedFileSize = 10 * 1024 * 1024;

    private int compactionScheduleInternal = 15 * 60 * 1000;

    private int maxOffsetMapSize = 100 * 1024 * 1024;

    private int compactionThreadNum = 6;

    private boolean enableCompaction = true;

    private int mappedFileSizeTimerLog = 100 * 1024 * 1024;

    private int timerPrecisionMs = 1000;

    private int timerRollWindowSlot = 3600 * 24 * 2;

    private int timerFlushIntervalMs = 1000;

    private int timerGetMessageThreadNum = 3;

    private int timerPutMessageThreadNum = 3;

    private boolean timerEnableDisruptor = false;

    private boolean timerEnableCheckMetrics = true;

    private boolean timerInterceptDelayLevel = false;

    private int timerMaxDelaySec = 3600 * 24 * 3;

    private boolean timerWheelEnable = true;

    @ImportantField
    private int disappearTimeAfterStart = -1;

    private boolean timerStopEnqueue = false;

    private String timerCheckMetricsWhen = "05";

    private boolean timerSkipUnknownError = false;

    private boolean timerWarmEnable = false;

    private boolean timerStopDequeue = false;

    private int timerCongestNumEachSlot = Integer.MAX_VALUE;

    private int timerMetricSmallThreshold = 1000000;

    private int timerProgressLogIntervalMs = 10 * 1000;

    @ImportantField
    private String storeType = StoreType.DEFAULT.getStoreType();

    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;

    private boolean enableConsumeQueueExt = false;

    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;

    private int mapperFileSizeBatchConsumeQueue = 300000 * BatchConsumeQueue.CQ_STORE_UNIT_SIZE;

    private int bitMapLengthConsumeQueueExt = 64;

    @ImportantField
    private int flushIntervalCommitLog = 500;

    @ImportantField
    private int commitIntervalCommitLog = 200;

    private int maxRecoveryCommitlogFiles = 30;

    private int diskSpaceWarningLevelRatio = 90;

    private int diskSpaceCleanForciblyRatio = 85;

    private boolean useReentrantLockWhenPutMessage = true;

    @ImportantField
    private boolean flushCommitLogTimed = true;

    private int flushIntervalConsumeQueue = 1000;

    private int cleanResourceInterval = 10000;

    private int deleteCommitLogFilesInterval = 100;

    private int deleteConsumeQueueFilesInterval = 100;

    private int destroyMapedFileIntervalForcibly = 1000 * 120;

    private int redeleteHangedFileInterval = 1000 * 120;

    @ImportantField
    private String deleteWhen = "04";

    private int diskMaxUsedSpaceRatio = 75;

    @ImportantField
    private int fileReservedTime = 72;

    @ImportantField
    private int deleteFileBatchMax = 10;

    private int putMsgIndexHightWater = 600000;

    private int maxMessageSize = 1024 * 1024 * 4;

    private int maxFilterMessageSize = 16000;

    private boolean checkCRCOnRecover = true;

    private int flushCommitLogLeastPages = 4;

    private int commitCommitLogLeastPages = 4;

    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;

    private int flushConsumeQueueLeastPages = 2;

    private int flushCommitLogThoroughInterval = 1000 * 10;

    private int commitCommitLogThoroughInterval = 200;

    private int flushConsumeQueueThoroughInterval = 1000 * 60;

    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;

    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;

    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;

    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;

    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;

    @ImportantField
    private boolean messageIndexEnable = true;

    private int maxHashSlotNum = 5000000;

    private int maxIndexNum = 5000000 * 4;

    private int maxMsgsNumBatch = 64;

    @ImportantField
    private boolean messageIndexSafe = false;

    private int haListenPort = 10912;

    private int haSendHeartbeatInterval = 1000 * 5;

    private int haHousekeepingInterval = 1000 * 20;

    private int haTransferBatchSize = 1024 * 32;

    @ImportantField
    private String haMasterAddress = null;

    private int haMaxGapNotInSync = 1024 * 1024 * 256;

    @ImportantField
    private volatile BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;

    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;

    private int syncFlushTimeout = 1000 * 5;

    private int putMessageTimeout = 1000 * 8;

    private int slaveTimeout = 3000;

    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

    private long flushDelayOffsetInterval = 1000 * 10;

    @ImportantField
    private boolean cleanFileForciblyEnable = true;

    private boolean warmMapedFileEnable = false;

    private boolean offsetCheckInSlave = false;

    private boolean debugLockEnable = false;

    private boolean duplicationEnable = false;

    private boolean diskFallRecorded = true;

    private long osPageCacheBusyTimeOutMills = 1000;

    private int defaultQueryMaxNum = 32;

    @ImportantField
    private boolean transientStorePoolEnable = false;

    private int transientStorePoolSize = 5;

    private boolean fastFailIfNoBufferInStorePool = false;

    private boolean enableDLegerCommitLog = false;

    private String dLegerGroup;

    private String dLegerPeers;

    private String dLegerSelfId;

    private String preferredLeaderId;

    private boolean enableBatchPush = false;

    private boolean enableScheduleMessageStats = true;

    private boolean enableLmq = false;

    private boolean enableMultiDispatch = false;

    private int maxLmqConsumeQueueNum = 20000;

    private boolean enableScheduleAsyncDeliver = false;

    private int scheduleAsyncDeliverMaxPendingLimit = 2000;

    private int scheduleAsyncDeliverMaxResendNum2Blocked = 3;

    private int maxBatchDeleteFilesNum = 50;

    private int dispatchCqThreads = 10;

    private int dispatchCqCacheNum = 1024 * 4;

    private boolean enableAsyncReput = true;

    private boolean recheckReputOffsetFromCq = false;

    @Deprecated
    private int maxTopicLength = Byte.MAX_VALUE;

    private boolean autoMessageVersionOnTopicLen = true;

    private boolean enabledAppendPropCRC = false;

    private boolean forceVerifyPropCRC = false;

    private int travelCqFileNumWhenGetMessage = 1;

    private int correctLogicMinOffsetSleepInterval = 1;

    private int correctLogicMinOffsetForceInterval = 5 * 60 * 1000;

    private boolean mappedFileSwapEnable = true;

    private long commitLogForceSwapMapInterval = 12L * 60 * 60 * 1000;

    private long commitLogSwapMapInterval = (long) 60 * 60 * 1000;

    private int commitLogSwapMapReserveFileNum = 100;

    private long logicQueueForceSwapMapInterval = 12L * 60 * 60 * 1000;

    private long logicQueueSwapMapInterval = (long) 60 * 60 * 1000;

    private long cleanSwapedMapInterval = 5L * 60 * 1000;

    private int logicQueueSwapMapReserveFileNum = 20;

    private boolean searchBcqByCacheEnable = true;

    @ImportantField
    private boolean dispatchFromSenderThread = false;

    @ImportantField
    private boolean wakeCommitWhenPutMessage = true;

    @ImportantField
    private boolean wakeFlushWhenPutMessage = false;

    @ImportantField
    private boolean enableCleanExpiredOffset = false;

    private int maxAsyncPutMessageRequests = 5000;

    private int pullBatchMaxMessageCount = 160;

    @ImportantField
    private int totalReplicas = 1;

    @ImportantField
    private int inSyncReplicas = 1;

    @ImportantField
    private int minInSyncReplicas = 1;

    @ImportantField
    private boolean allAckInSyncStateSet = false;

    @ImportantField
    private boolean enableAutoInSyncReplicas = false;

    @ImportantField
    private boolean haFlowControlEnable = false;

    private long maxHaTransferByteInSecond = 100 * 1024 * 1024;

    private long haMaxTimeSlaveNotCatchup = 1000 * 15;

    private boolean syncMasterFlushOffsetWhenStartup = false;

    private long maxChecksumRange = 1024 * 1024 * 1024;

    private int replicasPerDiskPartition = 1;

    private double logicalDiskSpaceCleanForciblyThreshold = 0.8;

    private long maxSlaveResendLength = 256 * 1024 * 1024;

    private boolean syncFromLastFile = false;

    private boolean asyncLearner = false;

    private int maxConsumeQueueScan = 20_000;

    private int sampleCountThreshold = 5000;

    private boolean coldDataFlowControlEnable = false;

    private boolean coldDataScanEnable = false;

    private boolean dataReadAheadEnable = true;

    private int timerColdDataCheckIntervalMs = 60 * 1000;

    private int sampleSteps = 32;

    private int accessMessageInMemoryHotRatio = 26;

    private boolean enableBuildConsumeQueueConcurrently = false;

    private int batchDispatchRequestThreadPoolNums = 16;

    private long cleanRocksDBDirtyCQIntervalMin = 60;

    private long statRocksDBCQIntervalSec = 10;

    private long memTableFlushIntervalMs = 60 * 60 * 1000L;

    private boolean realTimePersistRocksDBConfig = true;

    private boolean enableRocksDBLog = false;

    private int topicQueueLockNum = 32;

    private boolean readUnCommitted = false;

    public boolean isEnableRocksDBStore() {
        return StoreType.DEFAULT_ROCKSDB.getStoreType().equalsIgnoreCase(this.storeType);
    }

    public int getMappedFileSizeConsumeQueue() {
        int factor = (int) Math.ceil(this.mappedFileSizeConsumeQueue / (ConsumeQueue.CQ_STORE_UNIT_SIZE * 1.0));
        return factor * ConsumeQueue.CQ_STORE_UNIT_SIZE;
    }

    @Deprecated
    public int getMaxTopicLength() {
        return maxTopicLength;
    }

    @Deprecated
    public void setMaxTopicLength(int maxTopicLength) {
        this.maxTopicLength = maxTopicLength;
    }

    public String getStorePathCommitLog() {
        if (storePathCommitLog == null) {
            return storePathRootDir + File.separator + "commitlog";
        }
        return storePathCommitLog;
    }

    public String getStorePathEpochFile() {
        if (storePathEpochFile == null) {
            return storePathRootDir + File.separator + "epochFileCheckpoint";
        }
        return storePathEpochFile;
    }

    public String getStorePathBrokerIdentity() {
        if (storePathBrokerIdentity == null) {
            return storePathRootDir + File.separator + "brokerIdentity";
        }
        return storePathBrokerIdentity;
    }

    public int getDiskMaxUsedSpaceRatio() {
        if (this.diskMaxUsedSpaceRatio < 10)
            return 10;
        return Math.min(this.diskMaxUsedSpaceRatio, 95);
    }

    public void setHaListenPort(int haListenPort) {
        if (haListenPort < 0) {
            this.haListenPort = 0;
            return;
        }
        this.haListenPort = haListenPort;
    }

}
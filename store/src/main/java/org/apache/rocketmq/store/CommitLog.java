package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageExtEncoder.PutMessageThreadLocal;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.util.LibC;
import org.rocksdb.RocksDBException;
import sun.nio.ch.DirectBuffer;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 消息物理文件
 */
@Getter
@Setter
public class CommitLog implements Swappable {

    public final static int MESSAGE_MAGIC_CODE = -626843481;
    public final static int BLANK_MAGIC_CODE = -875286124;
    public static final int CRC32_RESERVED_LEN = MessageConst.PROPERTY_CRC32.length() + 1 + 10 + 1;
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    protected final MappedFileQueue mappedFileQueue;

    protected final DefaultMessageStore defaultMessageStore;
    protected final PutMessageLock putMessageLock;
    protected final TopicQueueLock topicQueueLock;
    protected final MultiDispatch multiDispatch;
    private final FlushManager flushManager;
    private final ColdDataCheckService coldDataCheckService;
    private final AppendMessageCallback appendMessageCallback;
    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;
    private final FlushDiskWatcher flushDiskWatcher;
    private final boolean enabledAppendPropCRC;
    protected volatile long confirmOffset = -1L;
    /**
     * 文件大小，默认为1G
     */
    protected int commitLogSize;
    private volatile long beginTimeInLock = 0;
    private volatile Set<String> fullStorePaths = Collections.emptySet();

    public CommitLog(final DefaultMessageStore messageStore) {
        //消息commitLog存储路径
        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        if (storePath.contains(MixAll.MULTI_PATH_SPLITTER)) {
            this.mappedFileQueue = new MultiPathMappedFileQueue(messageStore.getMessageStoreConfig(), messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(), messageStore.getAllocateMappedFileService(), this::getFullStorePaths);
        } else {
            this.mappedFileQueue = new MappedFileQueue(storePath, messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(), messageStore.getAllocateMappedFileService());
        }
        this.defaultMessageStore = messageStore;
        this.flushManager = new DefaultFlushManager();
        this.coldDataCheckService = new ColdDataCheckService();
        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig());
        putMessageThreadLocal = ThreadLocal.withInitial(() -> new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig()));
        this.putMessageLock = messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();
        this.flushDiskWatcher = new FlushDiskWatcher();
        this.topicQueueLock = new TopicQueueLock(messageStore.getMessageStoreConfig().getTopicQueueLockNum());
        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        this.enabledAppendPropCRC = messageStore.getMessageStoreConfig().isEnabledAppendPropCRC();
        this.multiDispatch = new MultiDispatch(defaultMessageStore);
    }

    public static boolean isMultiDispatchMsg(MessageExtBrokerInner msg) {
        return StringUtils.isNoneBlank(msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH)) && !msg.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
    }

    /**
     * 文件加载，将磁盘文件加载到mappedFileQueue
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        if (result && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable()) {
            scanFileAndSetReadMode(LibC.MADV_RANDOM);
        }
        this.mappedFileQueue.checkSelf();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * 启动
     */
    public void start() {
        this.flushManager.start();
        log.info("start commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.setDaemon(true);
        flushDiskWatcher.start();
        if (this.coldDataCheckService != null) {
            this.coldDataCheckService.start();
        }
    }

    /**
     * 关闭
     */
    public void shutdown() {
        this.flushManager.shutdown();
        log.info("shutdown commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.shutdown(true);
        if (this.coldDataCheckService != null) {
            this.coldDataCheckService.shutdown();
        }
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    /**
     * 获取刷盘指针
     */
    public long getFlushedWhere() {
        return this.mappedFileQueue.getFlushedWhere();
    }

    /**
     * 获取消息最大指针
     */
    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    /**
     * 计算剩余多少数据待提交
     */
    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    /**
     * 计算剩余多少数据待刷盘
     */
    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    /**
     * 删除过期文件
     */
    public int deleteExpiredFile(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, final boolean cleanImmediately) {
        return deleteExpiredFile(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, 0);
    }

    /**
     * 删除过期文件
     */
    public int deleteExpiredFile(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, final boolean cleanImmediately, final int deleteFileBatchMax) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, deleteFileBatchMax);
    }

    /**
     * 从指定偏移量获取数据
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    /**
     * 从指定偏移量获取数据
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos);
        }
        return null;
    }

    /**
     * 获取指定偏移量，指定大小的数据
     */
    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.getData(pos, size, byteBuffer);
        }
        return false;
    }

    public List<SelectMappedBufferResult> getBulkData(final long offset, final int size) {
        List<SelectMappedBufferResult> bufferResultList = new ArrayList<>();
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        int remainSize = size;
        long startOffset = offset;
        long maxOffset = this.getMaxOffset();
        if (offset + size > maxOffset) {
            remainSize = (int) (maxOffset - offset);
            log.warn("get bulk data size out of range, correct to max offset. offset: {}, size: {}, max: {}", offset, remainSize, maxOffset);
        }
        while (remainSize > 0) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(startOffset, startOffset == 0);
            if (mappedFile != null) {
                int pos = (int) (startOffset % mappedFileSize);
                int readableSize = mappedFile.getReadPosition() - pos;
                int readSize = Math.min(remainSize, readableSize);
                SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos, readSize);
                if (bufferResult == null) {
                    break;
                }
                bufferResultList.add(bufferResult);
                remainSize -= readSize;
                startOffset += readSize;
            }
        }
        return bufferResultList;
    }

    /**
     * 获取最后一个mappedFile，没有则创建一个
     */
    public boolean getLastMappedFile(final long startOffset) {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
        if (null == lastMappedFile) {
            log.error("getLastMappedFile error. offset:{}", startOffset);
            return false;
        }
        return true;
    }

    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long lastValidMsgPhyOffset = this.getConfirmOffset();
            boolean doDispatch = false;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();
                if (dispatchRequest.isSuccess() && size > 0) {
                    lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                    mappedFileOffset += size;
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                } else if (dispatchRequest.isSuccess() && size == 0) {
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                    index++;
                    if (index >= mappedFiles.size()) {
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                } else if (!dispatchRequest.isSuccess()) {
                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }
            processOffset += mappedFileOffset;
            if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                    log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                    this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
                } else if (this.defaultMessageStore.getConfirmOffset() > processOffset) {
                    log.error("confirmOffset {} is larger than processOffset {}, correct confirmOffset to processOffset", this.defaultMessageStore.getConfirmOffset(), processOffset);
                    this.defaultMessageStore.setConfirmOffset(processOffset);
                }
            } else {
                this.setConfirmOffset(lastValidMsgPhyOffset);
            }
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        } else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.getQueueStore().destroy();
            this.defaultMessageStore.getQueueStore().loadAfterDestroy();
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC, final boolean checkDupInfo) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * 检查消息，并返回消息大小
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC, final boolean checkDupInfo, final boolean readBody) {
        try {
            //4个字节的消息总大小
            int totalSize = byteBuffer.getInt();
            //魔数
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MessageDecoder.MESSAGE_MAGIC_CODE:
                case MessageDecoder.MESSAGE_MAGIC_CODE_V2:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false);
            }
            MessageVersion messageVersion = MessageVersion.valueOfMagicCode(magicCode);
            byte[] bytesContent = new byte[totalSize];
            int bodyCRC = byteBuffer.getInt();
            int queueId = byteBuffer.getInt();
            int flag = byteBuffer.getInt();
            long queueOffset = byteBuffer.getLong();
            long physicOffset = byteBuffer.getLong();
            int sysFlag = byteBuffer.getInt();
            long bornTimeStamp = byteBuffer.getLong();
            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            long storeTimestamp = byteBuffer.getLong();
            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            int reconsumeTimes = byteBuffer.getInt();
            long preparedTransactionOffset = byteBuffer.getLong();
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);
                    if (checkCRC) {
                        if (!this.defaultMessageStore.getMessageStoreConfig().isForceVerifyPropCRC()) {
                            int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                            if (crc != bodyCRC) {
                                log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                                return new DispatchRequest(-1, false/* success */);
                            }
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }
            int topicLen = messageVersion.getTopicLength(byteBuffer);
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);
            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);
                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (checkDupInfo) {
                    String dupInfo = propertiesMap.get(MessageConst.DUP_INFO);
                    if (null == dupInfo || dupInfo.split("_").length != 2) {
                        log.warn("DupInfo in properties check failed. dupInfo={}", dupInfo);
                        return new DispatchRequest(-1, false);
                    }
                }
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && !tags.isEmpty()) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(tags);
                }
                String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
                    int delayLevel = Integer.parseInt(t);
                    if (delayLevel > this.defaultMessageStore.getMaxDelayLevel()) {
                        delayLevel = this.defaultMessageStore.getMaxDelayLevel();
                    }
                    if (delayLevel > 0) {
                        tagsCode = this.defaultMessageStore.computeDeliverTimestamp(delayLevel, storeTimestamp);
                    }
                }
            }
            if (checkCRC) {
                if (this.defaultMessageStore.getMessageStoreConfig().isForceVerifyPropCRC()) {
                    int expectedCRC = -1;
                    if (propertiesMap != null) {
                        String crc32Str = propertiesMap.get(MessageConst.PROPERTY_CRC32);
                        if (crc32Str != null) {
                            expectedCRC = 0;
                            for (int i = crc32Str.length() - 1; i >= 0; i--) {
                                int num = crc32Str.charAt(i) - '0';
                                expectedCRC *= 10;
                                expectedCRC += num;
                            }
                        }
                    }
                    if (expectedCRC > 0) {
                        ByteBuffer tmpBuffer = byteBuffer.duplicate();
                        tmpBuffer.position(tmpBuffer.position() - totalSize);
                        tmpBuffer.limit(tmpBuffer.position() + totalSize - CommitLog.CRC32_RESERVED_LEN);
                        int crc = UtilAll.crc32(tmpBuffer);
                        if (crc != expectedCRC) {
                            log.warn("CommitLog#checkAndDispatchMessage: failed to check message CRC, expected " + "CRC={}, actual CRC={}", bodyCRC, crc);
                            return new DispatchRequest(-1, false);
                        }
                    } else {
                        log.warn("CommitLog#checkAndDispatchMessage: failed to check message CRC, not found CRC in properties");
                        return new DispatchRequest(-1, false);
                    }
                }
            }
            int readLength = MessageExtEncoder.calMsgLength(messageVersion, sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error("[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}", totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false);
            }
            DispatchRequest dispatchRequest = new DispatchRequest(topic, queueId, physicOffset, totalSize, tagsCode, storeTimestamp, queueOffset, keys, uniqKey, sysFlag, preparedTransactionOffset, propertiesMap);
            setBatchSizeIfNeeded(propertiesMap, dispatchRequest);
            return dispatchRequest;
        } catch (Exception ignore) {
        }
        return new DispatchRequest(-1, false);
    }

    private void setBatchSizeIfNeeded(Map<String, String> propertiesMap, DispatchRequest dispatchRequest) {
        if (null != propertiesMap && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_NUM) && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_BASE)) {
            dispatchRequest.setMsgBaseOffset(Long.parseLong(propertiesMap.get(MessageConst.PROPERTY_INNER_BASE)));
            dispatchRequest.setBatchSize(Short.parseShort(propertiesMap.get(MessageConst.PROPERTY_INNER_NUM)));
        }
    }

    /**
     * 获取确认偏移量
     */
    public long getConfirmOffset() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
                if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1 || !this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                    return this.defaultMessageStore.getMaxPhyOffset();
                }
                if (this.confirmOffset < 0) {
                    setConfirmOffset(((AutoSwitchHAService) this.defaultMessageStore.getHaService()).computeConfirmOffset());
                    log.info("Init the confirmOffset to {}.", this.confirmOffset);
                }
            }
            return this.confirmOffset;
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return this.defaultMessageStore.isSyncDiskFlush() ? getFlushedWhere() : getMaxOffset();
        }
    }

    /**
     * 设置确认偏移量
     */
    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
        this.defaultMessageStore.getStoreCheckpoint().setConfirmPhyOffset(confirmOffset);
    }

    public long getConfirmOffsetDirectly() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
                if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1) {
                    return this.defaultMessageStore.getMaxPhyOffset();
                }
            }
            return this.confirmOffset;
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    /**
     * 获取最后一个文件的起始偏移量
     */
    public long getLastFileFromOffset() {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (lastMappedFile != null) {
            if (lastMappedFile.isAvailable()) {
                return lastMappedFile.getFileFromOffset();
            }
        }
        return -1;
    }

    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }
            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long lastValidMsgPhyOffset = processOffset;
            long lastConfirmValidMsgPhyOffset = processOffset;
            boolean doDispatch = true;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();
                if (dispatchRequest.isSuccess()) {
                    if (size > 0) {
                        lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                        mappedFileOffset += size;
                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable() || this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                            if (dispatchRequest.getCommitLogOffset() + size <= this.defaultMessageStore.getCommitLog().getConfirmOffset()) {
                                this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                                lastConfirmValidMsgPhyOffset = dispatchRequest.getCommitLogOffset() + size;
                            }
                        } else {
                            this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    else if (size == 0) {
                        this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                        index++;
                        if (index >= mappedFiles.size()) {
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {
                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }
                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }
            this.getMessageStore().finishCommitLogDispatch();
            processOffset += mappedFileOffset;
            if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                    log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                    this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
                } else if (this.defaultMessageStore.getConfirmOffset() > lastConfirmValidMsgPhyOffset) {
                    log.error("confirmOffset {} is larger than lastConfirmValidMsgPhyOffset {}, correct confirmOffset to lastConfirmValidMsgPhyOffset", this.defaultMessageStore.getConfirmOffset(), lastConfirmValidMsgPhyOffset);
                    this.defaultMessageStore.setConfirmOffset(lastConfirmValidMsgPhyOffset);
                }
            } else {
                this.setConfirmOffset(lastValidMsgPhyOffset);
            }
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        } else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.getQueueStore().destroy();
            this.defaultMessageStore.getQueueStore().loadAfterDestroy();
        }
    }

    public void truncateDirtyFiles(long phyOffset) {
        if (phyOffset <= this.getFlushedWhere()) {
            this.mappedFileQueue.setFlushedWhere(phyOffset);
        }
        if (phyOffset <= this.mappedFileQueue.getCommittedWhere()) {
            this.mappedFileQueue.setCommittedWhere(phyOffset);
        }
        this.mappedFileQueue.truncateDirtyFiles(phyOffset);
        if (this.confirmOffset > phyOffset) {
            this.setConfirmOffset(phyOffset);
        }
    }

    protected void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        this.getMessageStore().onCommitLogAppend(msg, result, commitLogFile);
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) throws RocksDBException {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSITION);
        if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE && magicCode != MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
            return false;
        }
        if (this.defaultMessageStore.getMessageStoreConfig().isEnableRocksDBStore()) {
            final long maxPhyOffsetInConsumeQueue = this.defaultMessageStore.getQueueStore().getMaxPhyOffsetInConsumeQueue();
            long phyOffset = byteBuffer.getLong(MessageDecoder.MESSAGE_PHYSIC_OFFSET_POSITION);
            if (phyOffset <= maxPhyOffsetInConsumeQueue) {
                log.info("find check. beginPhyOffset: {}, maxPhyOffsetInConsumeQueue: {}", phyOffset, maxPhyOffsetInConsumeQueue);
                return true;
            }
        } else {
            int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
            int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornHostLength;
            long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
            if (0 == storeTimestamp) {
                return false;
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable() && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
                if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                    log.info("find check timestamp, {} {}", storeTimestamp, UtilAll.timeMillisToHumanString(storeTimestamp));
                    return true;
                }
            } else {
                if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                    log.info("find check timestamp, {} {}", storeTimestamp, UtilAll.timeMillisToHumanString(storeTimestamp));
                    return true;
                }
            }
        }
        return false;
    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    public void setMappedFileQueueOffset(final long phyOffset) {
        this.mappedFileQueue.setFlushedWhere(phyOffset);
        this.mappedFileQueue.setCommittedWhere(phyOffset);
    }

    public void updateMaxMessageSize(PutMessageThreadLocal putMessageThreadLocal) {
        int newMaxMessageSize = this.defaultMessageStore.getMessageStoreConfig().getMaxMessageSize();
        if (newMaxMessageSize >= 10) {
            putMessageThreadLocal.getEncoder().updateEncoderBufferCapacity(newMaxMessageSize);
        }
    }

    /**
     * commitLog消息存储
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        //设置消息存储时间
        if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            msg.setStoreTimestamp(System.currentTimeMillis());
        }
        //设置消息crc
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        if (enabledAppendPropCRC) {
            //删除crc32属性
            msg.deleteProperty(MessageConst.PROPERTY_CRC32);
        }

        AppendMessageResult result;
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();
        String topic = msg.getTopic();
        msg.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen = this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && topic.length() > Byte.MAX_VALUE) {
            msg.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }
        //生成消息的客户端地址
        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }
        //消息存储地址
        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }
        PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(putMessageThreadLocal);
        //生成关于topic和queue的key
        String topicQueueKey = generateKey(putMessageThreadLocal.getKeyBuilder(), msg);
        long elapsedTimeInLock;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        //当前位置指针
        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            //文件开始偏移量 + 当前文件写指针位置
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        //需要多副本ack的数量
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(msg);
        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                //means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(), this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        topicQueueLock.lock(topicQueueKey);
        try {
            boolean needAssignOffset = !defaultMessageStore.getMessageStoreConfig().isDuplicationEnable() || defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;
            if (needAssignOffset) {
                defaultMessageStore.assignOffset(msg);
            }
            PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
            if (encodeResult != null) {
                return CompletableFuture.completedFuture(encodeResult);
            }
            msg.setEncodedBuff(putMessageThreadLocal.getEncoder().getEncoderBuffer());
            PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);
            putMessageLock.lock();
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;
                //设置消息存储时间，以保证全局的顺序
                if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                    msg.setStoreTimestamp(beginLockTimestamp);
                }
                if (null == mappedFile || mappedFile.isFull()) {
                    //获取最后一个mappedFile，没有则创建一个
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (isCloseReadAhead()) {
                        setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                    }
                }
                if (null == mappedFile) {
                    log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                //获取或者创建mappedFile成功后，往当前mappedFile追加消息
                result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        onCommitLogAppend(msg, result, mappedFile);
                        break;
                    case END_OF_FILE:
                        onCommitLogAppend(msg, result, mappedFile);
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        if (isCloseReadAhead()) {
                            setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                        }
                        result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                        if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                            onCommitLogAppend(msg, result, mappedFile);
                        }
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }
            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                this.defaultMessageStore.increaseOffset(msg, getMessageNum(msg));
            }
        } catch (RocksDBException e) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null));
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).add(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).add(result.getWroteBytes());
        return handleDiskFlushAndHA(putMessageResult, msg, needAckNums, needHandleHA);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }

        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(messageExtBatch);

        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                    this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen =
                this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        //fine-grained lock instead of the coarse-grained
        PutMessageThreadLocal pmThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(pmThreadLocal);
        MessageExtEncoder batchEncoder = pmThreadLocal.getEncoder();

        String topicQueueKey = generateKey(pmThreadLocal.getKeyBuilder(), messageExtBatch);

        PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch, putMessageContext));

        topicQueueLock.lock(topicQueueKey);
        try {
            defaultMessageStore.assignOffset(messageExtBatch);

            putMessageLock.lock();
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                messageExtBatch.setStoreTimestamp(beginLockTimestamp);

                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                    if (isCloseReadAhead()) {
                        setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                    }
                }
                if (null == mappedFile) {
                    log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        break;
                    case END_OF_FILE:
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        if (isCloseReadAhead()) {
                            setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                        }
                        result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }

            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                this.defaultMessageStore.increaseOffset(messageExtBatch, (short) putMessageContext.getBatchSize());
            }
        } catch (RocksDBException e) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null));
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).add(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).add(result.getWroteBytes());

        return handleDiskFlushAndHA(putMessageResult, messageExtBatch, needAckNums, needHandleHA);
    }

    private int calcNeedAckNums(int inSyncReplicas) {
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        if (this.defaultMessageStore.getMessageStoreConfig().isEnableAutoInSyncReplicas()) {
            needAckNums = Math.min(needAckNums, inSyncReplicas);
            needAckNums = Math.max(needAckNums, this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas());
        }
        return needAckNums;
    }

    /**
     * 是否需要同步主从服务
     */
    private boolean needHandleHA(MessageExt messageExt) {
        if (!messageExt.isWaitStoreMsgOK()) {
            return false;
        }
        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return false;
        }
        return BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole();
    }

    private CompletableFuture<PutMessageResult> handleDiskFlushAndHA(PutMessageResult putMessageResult,
                                                                     MessageExt messageExt, int needAckNums, boolean needHandleHA) {
        CompletableFuture<PutMessageStatus> flushResultFuture = handleDiskFlush(putMessageResult.getAppendMessageResult(), messageExt);
        CompletableFuture<PutMessageStatus> replicaResultFuture;
        if (!needHandleHA) {
            replicaResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        } else {
            replicaResultFuture = handleHA(putMessageResult.getAppendMessageResult(), needAckNums);
        }

        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });
    }

    private CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
        return this.flushManager.handleDiskFlush(result, messageExt);
    }

    private CompletableFuture<PutMessageStatus> handleHA(AppendMessageResult result, int needAckNums) {
        if (needAckNums >= 0 && needAckNums <= 1) {
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }

        HAService haService = this.defaultMessageStore.getHaService();

        long nextOffset = result.getWroteOffset() + result.getWroteBytes();

        // Wait enough acks from different slaves
        GroupCommitRequest request = new GroupCommitRequest(nextOffset, this.defaultMessageStore.getMessageStoreConfig().getSlaveTimeout(), needAckNums);
        haService.putRequest(request);
        haService.getWaitNotifyObject().wakeupAll();
        return request.future();
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset() && offset + size <= this.getMaxOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }
        return -1;
    }

    /**
     * 获取最小偏移量
     */
    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }
        return -1;
    }

    /**
     * 根据偏移量和消息长度查找消息
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            //计算得到消息在mappedFile中的起始位置
            int pos = (int) (offset % mappedFileSize);
            //根据消息起始位置和长度查找消息
            SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(pos, size);
            if (null != selectMappedBufferResult) {
                selectMappedBufferResult.setInCache(coldDataCheckService.isDataInPageCache(offset));
                return selectMappedBufferResult;
            }
        }
        return null;
    }

    /**
     * 获取下一个文件的起始偏移量
     */
    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data, dataStart, dataLength);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    protected short getMessageNum(MessageExtBrokerInner msgInner) {
        short messageNum = 1;
        // IF inner batch, build batchQueueOffset and batchNum property.
        CQType cqType = getCqType(msgInner);

        if (MessageSysFlag.check(msgInner.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) || CQType.BatchCQ.equals(cqType)) {
            if (msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM) != null) {
                messageNum = Short.parseShort(msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM));
                messageNum = messageNum >= 1 ? messageNum : 1;
            }
        }

        return messageNum;
    }

    private CQType getCqType(MessageExtBrokerInner msgInner) {
        Optional<TopicConfig> topicConfig = this.defaultMessageStore.getTopicConfig(msgInner.getTopic());
        return QueueTypeUtils.getCQType(topicConfig);
    }

    public long getTotalSize() {
        return 0L;
    }

    public MessageStore getMessageStore() {
        return defaultMessageStore;
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        this.getMappedFileQueue().swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFileQueue.isMappedFilesEmpty();
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        this.getMappedFileQueue().cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    private boolean isCloseReadAhead() {
        return !MixAll.isWindows() && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable();
    }

    /**
     * 扫面并设置文件读写模式
     */
    public void scanFileAndSetReadMode(int mode) {
        if (MixAll.isWindows()) {
            log.info("windows os stop scanFileAndSetReadMode");
            return;
        }
        try {
            log.info("scanFileAndSetReadMode mode: {}", mode);
            mappedFileQueue.getMappedFiles().forEach(mappedFile -> setFileReadMode(mappedFile, mode));
        } catch (Exception e) {
            log.error("scanFileAndSetReadMode exception", e);
        }
    }

    /**
     * 设置文件读写模式
     */
    private void setFileReadMode(MappedFile mappedFile, int mode) {
        if (null == mappedFile) {
            log.error("setFileReadMode mappedFile is null");
            return;
        }
        final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
        int mAdvise = LibC.INSTANCE.madvise(new Pointer(address), new NativeLong(mappedFile.getFileSize()), mode);
        if (mAdvise != 0) {
            log.error("setFileReadMode error fileName: {}, madvise: {}, mode:{}", mappedFile.getFileName(), mAdvise, mode);
        }
    }

    abstract static class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    @Getter
    @Setter
    public static class GroupCommitRequest {
        private final long nextOffset;
        // Indicate the GroupCommitRequest result: true or false
        private final CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private final long deadLine;
        private volatile int ackNums = 1;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
        }

        public GroupCommitRequest(long nextOffset, long timeoutMillis, int ackNums) {
            this(nextOffset, timeoutMillis);
            this.ackNums = ackNums;
        }

        public void wakeupCustomer(final PutMessageStatus status) {
            this.flushOKFuture.complete(status);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }
    }

    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerIdentity().getIdentifier() + CommitRealTimeService.class.getSimpleName();
            }
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                int commitDataThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        CommitLog.this.flushManager.wakeUpFlush();
                    }
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("COMMIT_DATA_TIME_MS", (int) (end - begin));
                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("FLUSH_DATA_TIME_MS", (int) past);
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + FlushRealTimeService.class.getSimpleName();
            }
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        private final PutMessageSpinLock lock = new PutMessageSpinLock();
        private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<>();
        private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<>();

        public void putRequest(final GroupCommitRequest request) {
            lock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            this.wakeup();
        }

        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    for (int i = 0; i < 1000 && !flushOK; i++) {
                        CommitLog.this.mappedFileQueue.flush(0);
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        if (flushOK) {
                            break;
                        } else {
                            // When transientStorePoolEnable is true, the messages in writeBuffer may not be committed
                            // to pageCache very quickly, and flushOk here may almost be false, so we can sleep 1ms to
                            // wait for the messages to be committed to pageCache.
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }

                    req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }

                long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }

                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            this.swapRequests();
            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCommitService.class.getSimpleName();
            }
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        private final int crc32ReservedLength = CommitLog.CRC32_RESERVED_LEN;
        private final MessageStoreConfig messageStoreConfig;

        DefaultAppendMessageCallback(MessageStoreConfig messageStoreConfig) {
            this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
            this.messageStoreConfig = messageStoreConfig;
        }

        public AppendMessageResult handlePropertiesForLmqMsg(ByteBuffer preEncodeBuffer,
                                                             final MessageExtBrokerInner msgInner) {
            if (msgInner.isEncodeCompleted()) {
                return null;
            }

            multiDispatch.wrapMultiDispatch(msgInner);

            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            boolean needAppendLastPropertySeparator = enabledAppendPropCRC && propertiesData != null && propertiesData.length > 0
                    && propertiesData[propertiesData.length - 1] != MessageDecoder.PROPERTY_SEPARATOR;

            final int propertiesLength = (propertiesData == null ? 0 : propertiesData.length) + (needAppendLastPropertySeparator ? 1 : 0) + crc32ReservedLength;

            if (propertiesLength > Short.MAX_VALUE) {
                assert propertiesData != null;
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            int msgLenWithoutProperties = preEncodeBuffer.getInt(0);

            int msgLen = msgLenWithoutProperties + 2 + propertiesLength;

            // Exceeds the maximum message
            if (msgLen > this.messageStoreConfig.getMaxMessageSize()) {
                log.warn("message size exceeded, msg total size: " + msgLen + ", maxMessageSize: " + this.messageStoreConfig.getMaxMessageSize());
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Back filling total message length
            preEncodeBuffer.putInt(0, msgLen);
            // Modify position to msgLenWithoutProperties
            preEncodeBuffer.position(msgLenWithoutProperties);

            preEncodeBuffer.putShort((short) propertiesLength);

            if (propertiesLength > crc32ReservedLength) {
                preEncodeBuffer.put(propertiesData);
            }

            if (needAppendLastPropertySeparator) {
                preEncodeBuffer.put((byte) MessageDecoder.PROPERTY_SEPARATOR);
            }
            // 18 CRC32
            preEncodeBuffer.position(preEncodeBuffer.position() + crc32ReservedLength);

            msgInner.setEncodeCompleted(true);

            return null;
        }

        /**
         * 追加消息
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {
            ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
            boolean isMultiDispatchMsg = messageStoreConfig.isEnableMultiDispatch() && CommitLog.isMultiDispatchMsg(msgInner);
            if (isMultiDispatchMsg) {
                AppendMessageResult appendMessageResult = handlePropertiesForLmqMsg(preEncodeBuffer, msgInner);
                if (appendMessageResult != null) {
                    return appendMessageResult;
                }
            }
            final int msgLen = preEncodeBuffer.getInt(0);
            preEncodeBuffer.position(0);
            preEncodeBuffer.limit(msgLen);
            long wroteOffset = fileFromOffset + byteBuffer.position();
            Supplier<String> msgIdSupplier = () -> {
                int sysflag = msgInner.getSysFlag();
                int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();
                msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
                return UtilAll.bytes2string(msgIdBuffer.array());
            };
            long queueOffset = msgInner.getQueueOffset();
            short messageNum = getMessageNum(msgInner);
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.msgStoreItemMemory.clear();
                this.msgStoreItemMemory.putInt(maxBlank);
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }
            int pos = 4 + 4 + 4 + 4 + 4;
            preEncodeBuffer.putLong(pos, queueOffset);
            pos += 8;
            preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
            int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            pos += 8 + 4 + 8 + ipLen;
            preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());
            if (enabledAppendPropCRC) {
                int checkSize = msgLen - crc32ReservedLength;
                ByteBuffer tmpBuffer = preEncodeBuffer.duplicate();
                tmpBuffer.limit(tmpBuffer.position() + checkSize);
                int crc32 = UtilAll.crc32(tmpBuffer);
                tmpBuffer.limit(tmpBuffer.position() + crc32ReservedLength);
                MessageDecoder.createCrc32(tmpBuffer, crc32);
            }
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            CommitLog.this.getMessageStore().getPerfCounter().startTick("WRITE_MEMORY_TIME_MS");
            byteBuffer.put(preEncodeBuffer);
            CommitLog.this.getMessageStore().getPerfCounter().endTick("WRITE_MEMORY_TIME_MS");
            msgInner.setEncodedBuff(null);
            if (isMultiDispatchMsg) {
                CommitLog.this.multiDispatch.updateMultiQueueOffset(msgInner);
            }
            return new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgIdSupplier, msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills, messageNum);
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            long queueOffset = messageExtBatch.getQueueOffset();
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            Supplier<String> msgIdSupplier = () -> {
                int msgIdLen = storeHostLength + 8;
                int batchCount = putMessageContext.getBatchSize();
                long[] phyPosArray = putMessageContext.getPhyPos();
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer

                StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
                for (int i = 0; i < phyPosArray.length; i++) {
                    msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
                    String msgId = UtilAll.bytes2string(msgIdBuffer.array());
                    if (i != 0) {
                        buffer.append(',');
                    }
                    buffer.append(msgId);
                }
                return buffer.toString();
            };

            messagesByteBuff.mark();
            int index = 0;
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.msgStoreItemMemory.clear();
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, messageExtBatch.getStoreTimestamp(),
                            beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                int pos = msgPos + 20;
                messagesByteBuff.putLong(pos, queueOffset);
                pos += 8;
                messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
                // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
                pos += 8 + 4 + 8 + bornHostLength;
                // refresh store time stamp in lock
                messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());
                if (enabledAppendPropCRC) {
                    //append crc32
                    int checkSize = msgLen - crc32ReservedLength;
                    ByteBuffer tmpBuffer = messagesByteBuff.duplicate();
                    tmpBuffer.position(msgPos).limit(msgPos + checkSize);
                    int crc32 = UtilAll.crc32(tmpBuffer);
                    messagesByteBuff.position(msgPos + checkSize);
                    MessageDecoder.createCrc32(messagesByteBuff, crc32);
                }

                putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdSupplier,
                    messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);

            return result;
        }

    }

    class DefaultFlushManager implements FlushManager {

        private final FlushCommitLogService flushCommitLogService;

        /**
         * If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
         */
        private final FlushCommitLogService commitRealTimeService;

        public DefaultFlushManager() {
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                //同步刷盘
                this.flushCommitLogService = new CommitLog.GroupCommitService();
            } else {
                //异步刷盘
                this.flushCommitLogService = new CommitLog.FlushRealTimeService();
            }
            this.commitRealTimeService = new CommitLog.CommitRealTimeService();
        }

        @Override
        public void start() {
            this.flushCommitLogService.start();
            if (defaultMessageStore.isTransientStorePoolEnable()) {
                this.commitRealTimeService.start();
            }
        }

        @Override
        public CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    flushDiskWatcher.add(request);
                    service.putRequest(request);
                    return request.future();
                } else {
                    service.wakeup();
                    return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
                }
            }
            // Asynchronous flush
            else {
                if (!CommitLog.this.defaultMessageStore.isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                } else {
                    commitRealTimeService.wakeup();
                }
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }

        @Override
        public void wakeUpFlush() {
            // now wake up flush thread.
            flushCommitLogService.wakeup();
        }

        @Override
        public void wakeUpCommit() {
            // now wake up commit log thread.
            commitRealTimeService.wakeup();
        }

        @Override
        public void shutdown() {
            if (defaultMessageStore.isTransientStorePoolEnable()) {
                this.commitRealTimeService.shutdown();
            }

            this.flushCommitLogService.shutdown();
        }

    }

    public class ColdDataCheckService extends ServiceThread {
        private final SystemClock systemClock = new SystemClock();
        private final ConcurrentHashMap<String, byte[]> pageCacheMap = new ConcurrentHashMap<>();
        private int pageSize = -1;
        private int sampleSteps;

        public ColdDataCheckService() {
            sampleSteps = defaultMessageStore.getMessageStoreConfig().getSampleSteps();
            if (sampleSteps <= 0) {
                sampleSteps = 32;
            }
            initPageSize();
            scanFilesInPageCache();
        }

        @Override
        public String getServiceName() {
            return ColdDataCheckService.class.getSimpleName();
        }

        @Override
        public void run() {
            log.info("{} service started", this.getServiceName());
            while (!this.isStopped()) {
                try {
                    if (MixAll.isWindows() || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable() || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {
                        pageCacheMap.clear();
                        this.waitForRunning(180 * 1000);
                        continue;
                    } else {
                        this.waitForRunning(defaultMessageStore.getMessageStoreConfig().getTimerColdDataCheckIntervalMs());
                    }

                    if (pageSize < 0) {
                        initPageSize();
                    }

                    long beginClockTimestamp = this.systemClock.now();
                    scanFilesInPageCache();
                    long costTime = this.systemClock.now() - beginClockTimestamp;
                    log.info("[{}] scanFilesInPageCache-cost {} ms.", costTime > 30 * 1000 ? "NOTIFYME" : "OK", costTime);
                } catch (Throwable e) {
                    log.warn(this.getServiceName() + " service has e: {}", e);
                }
            }
            log.info("{} service end", this.getServiceName());
        }

        public boolean isDataInPageCache(final long offset) {
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                return true;
            }
            if (pageSize <= 0 || sampleSteps <= 0) {
                return true;
            }
            if (!defaultMessageStore.checkInColdAreaByCommitOffset(offset, getMaxOffset())) {
                return true;
            }
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {
                return false;
            }

            MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
            if (null == mappedFile) {
                return true;
            }
            byte[] bytes = pageCacheMap.get(mappedFile.getFileName());
            if (null == bytes) {
                return true;
            }

            int pos = (int) (offset % defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
            int realIndex = pos / pageSize / sampleSteps;
            return bytes.length - 1 >= realIndex && bytes[realIndex] != 0;
        }

        private void scanFilesInPageCache() {
            if (MixAll.isWindows() || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable() || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable() || pageSize <= 0) {
                return;
            }
            try {
                log.info("pageCacheMap key size: {}", pageCacheMap.size());
                clearExpireMappedFile();
                mappedFileQueue.getMappedFiles().forEach(mappedFile -> {
                    byte[] pageCacheTable = checkFileInPageCache(mappedFile);
                    if (sampleSteps > 1) {
                        pageCacheTable = sampling(pageCacheTable, sampleSteps);
                    }
                    pageCacheMap.put(mappedFile.getFileName(), pageCacheTable);
                });
            } catch (Exception e) {
                log.error("scanFilesInPageCache exception", e);
            }
        }

        private void clearExpireMappedFile() {
            Set<String> currentFileSet = mappedFileQueue.getMappedFiles().stream().map(MappedFile::getFileName).collect(Collectors.toSet());
            pageCacheMap.forEach((key, value) -> {
                if (!currentFileSet.contains(key)) {
                    pageCacheMap.remove(key);
                    log.info("clearExpireMappedFile fileName: {}, has been clear", key);
                }
            });
        }

        private byte[] sampling(byte[] pageCacheTable, int sampleStep) {
            byte[] sample = new byte[(pageCacheTable.length + sampleStep - 1) / sampleStep];
            for (int i = 0, j = 0; i < pageCacheTable.length && j < sample.length; i += sampleStep) {
                sample[j++] = pageCacheTable[i];
            }
            return sample;
        }

        private byte[] checkFileInPageCache(MappedFile mappedFile) {
            long fileSize = mappedFile.getFileSize();
            final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
            int pageNums = (int) (fileSize + this.pageSize - 1) / this.pageSize;
            byte[] pageCacheRst = new byte[pageNums];
            int mincore = LibC.INSTANCE.mincore(new Pointer(address), new NativeLong(fileSize), pageCacheRst);
            if (mincore != 0) {
                log.error("checkFileInPageCache call the LibC.INSTANCE.mincore error, fileName: {}, fileSize: {}",
                        mappedFile.getFileName(), fileSize);
                Arrays.fill(pageCacheRst, (byte) 1);
            }
            return pageCacheRst;
        }

        private void initPageSize() {
            if (pageSize < 0 && defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                try {
                    if (!MixAll.isWindows()) {
                        pageSize = LibC.INSTANCE.getpagesize();
                    } else {
                        defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
                        log.info("windows os, coldDataCheckEnable force setting to be false");
                    }
                    log.info("initPageSize pageSize: {}", pageSize);
                } catch (Exception e) {
                    defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
                    log.error("initPageSize error, coldDataCheckEnable force setting to be false ", e);
                }
            }
        }

        /**
         * this result is not high accurate.
         */
        public boolean isMsgInColdArea(String group, String topic, int queueId, long offset) {
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                return false;
            }
            try {
                ConsumeQueue consumeQueue = (ConsumeQueue) defaultMessageStore.findConsumeQueue(topic, queueId);
                if (null == consumeQueue) {
                    return false;
                }
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (null == bufferConsumeQueue || null == bufferConsumeQueue.getByteBuffer()) {
                    return false;
                }
                long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                return defaultMessageStore.checkInColdAreaByCommitOffset(offsetPy, getMaxOffset());
            } catch (Exception e) {
                log.error("isMsgInColdArea group: {}, topic: {}, queueId: {}, offset: {}",
                        group, topic, queueId, offset, e);
            }
            return false;
        }
    }

}
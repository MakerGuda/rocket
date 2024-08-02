package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

/**
 * 映射文件队列
 */
@Getter
@Setter
public class MappedFileQueue implements Swappable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * 文件存储路径
     */
    protected final String storePath;

    /**
     * mappedFile文件大小
     */
    protected final int mappedFileSize;

    /**
     * mappedFile文件集合
     */
    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    /**
     * 创建mappedFile服务类
     */
    protected final AllocateMappedFileService allocateMappedFileService;

    /**
     * 当前刷盘指针，表示该指针之前的数据全部持久化到磁盘
     */
    protected long flushedWhere = 0;

    /**
     * 当前数据提交指针，内存中ByteBuffer当前的写指针，该值大于等于flushedWhere
     */
    protected long committedWhere = 0;

    /**
     * 存储时间戳
     */
    protected volatile long storeTimestamp = 0;

    /**
     * 构造函数
     */
    public MappedFileQueue(final String storePath, int mappedFileSize, AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     * 自我检测
     */
    public void checkSelf() {
        List<MappedFile> mappedFiles = new ArrayList<>(this.mappedFiles);
        if (!mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (pre != null) {
                    //当前文件的起始偏移量 - 上个文件的起始偏移量 必须等于 单个文件大小，即每个文件大小相等
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}", pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    public MappedFile getConsumeQueueMappedFileByTime(final long timestamp, CommitLog commitLog, BoundaryType boundaryType) {
        Object[] mfs = copyMappedFiles();
        if (null == mfs) {
            return null;
        }
        /*
         * Make sure each mapped file in consume queue has accurate start and stop time in accordance with commit log
         * mapped files. Note last modified time from file system is not reliable.
         */
        for (int i = mfs.length - 1; i >= 0; i--) {
            DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
            // Figure out the earliest message store time in the consume queue mapped file.
            if (mappedFile.getStartTimestamp() < 0) {
                SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0, ConsumeQueue.CQ_STORE_UNIT_SIZE);
                if (null != selectMappedBufferResult) {
                    try {
                        ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
                        long physicalOffset = buffer.getLong();
                        int messageSize = buffer.getInt();
                        long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
                        if (messageStoreTime > 0) {
                            mappedFile.setStartTimestamp(messageStoreTime);
                        }
                    } finally {
                        selectMappedBufferResult.release();
                    }
                }
            }
            // Figure out the latest message store time in the consume queue mapped file.
            if (i < mfs.length - 1 && mappedFile.getStopTimestamp() < 0) {
                SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(mappedFileSize - ConsumeQueue.CQ_STORE_UNIT_SIZE, ConsumeQueue.CQ_STORE_UNIT_SIZE);
                if (null != selectMappedBufferResult) {
                    try {
                        ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
                        long physicalOffset = buffer.getLong();
                        int messageSize = buffer.getInt();
                        long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
                        if (messageStoreTime > 0) {
                            mappedFile.setStopTimestamp(messageStoreTime);
                        }
                    } finally {
                        selectMappedBufferResult.release();
                    }
                }
            }
        }

        switch (boundaryType) {
            case LOWER: {
                for (int i = 0; i < mfs.length; i++) {
                    DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
                    if (i < mfs.length - 1) {
                        long stopTimestamp = mappedFile.getStopTimestamp();
                        if (stopTimestamp >= timestamp) {
                            return mappedFile;
                        }
                    }

                    // Just return the latest one.
                    if (i == mfs.length - 1) {
                        return mappedFile;
                    }
                }
            }
            case UPPER: {
                for (int i = mfs.length - 1; i >= 0; i--) {
                    DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
                    if (mappedFile.getStartTimestamp() <= timestamp) {
                        return mappedFile;
                    }
                }
            }

            default: {
                log.warn("Unknown boundary type");
                break;
            }
        }
        return null;
    }

    /**
     * 将mappedFile列表转换为数组返回
     */
    protected Object[] copyMappedFiles() {
        Object[] mfs;
        if (this.mappedFiles.isEmpty()) {
            return null;
        }
        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<>();
        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }
        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 删除过期文件
     */
    void deleteExpiredFile(List<MappedFile> files) {
        if (!files.isEmpty()) {
            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }
            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * 从磁盘中加载文件
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] ls = dir.listFiles();
        if (ls != null) {
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    /**
     * 加载文件
     */
    public boolean doLoad(List<File> files) {
        //按照文件名称进行顺序排序，文件名称对应的是当前commitLog的起始偏移量
        files.sort(Comparator.comparing(File::getName));
        for (int i = 0; i < files.size(); i++) {
            File file = files.get(i);
            if (file.isDirectory()) {
                continue;
            }
            if (file.length() == 0 && i == files.size() - 1) {
                boolean ok = file.delete();
                log.warn("{} size is 0, auto delete. is_ok: {}", file, ok);
                continue;
            }
            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length() + " length not matched message store config value, please check it manually");
                return false;
            }
            try {
                MappedFile mappedFile = new DefaultMappedFile(file.getPath(), mappedFileSize);
                //初始化当前文件的写指针，刷盘指针，提交指针为单个文件大小
                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    /**
     * 获取最后一个mappedFile文件，没有则按需创建一个
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();
        //最后一个文件不存在，则按照startOffset计算创建的mappedFile起始位置
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        //最后一个mappedFile满了，则创建一个新的，起始位置 = 最后一个mappedFile起始位置 + 文件大小(每个文件固定大小)
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }
        //尝试创建一个新的
        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }
        return mappedFileLast;
    }

    /**
     * 判断mappedFiles是否为空
     */
    public boolean isMappedFilesEmpty() {
        return this.mappedFiles.isEmpty();
    }

    /**
     * 判断当前mappedFile队列中mappedFile文件是否为空，或者是否为满
     */
    public boolean isEmptyOrCurrentFileFull() {
        MappedFile mappedFileLast = getLastMappedFile();
        if (mappedFileLast == null) {
            return true;
        }
        return mappedFileLast.isFull();
    }

    /**
     * 从起始偏移量尝试创建一个新的mappedFile
     */
    public MappedFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
        String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    /**
     * 创建mappedFile
     */
    protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
        MappedFile mappedFile = null;
        if (this.allocateMappedFileService != null) {
            mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, this.mappedFileSize);
        } else {
            try {
                mappedFile = new DefaultMappedFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }
        }
        if (mappedFile != null) {
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            this.mappedFiles.add(mappedFile);
        }
        return mappedFile;
    }

    /**
     * 获取最后一个mappedFile文件，没有则从指定offset创建一个
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个mappedFile
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;
        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException ignored) {
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }
        return mappedFileLast;
    }

    /**
     * 重置偏移量
     */
    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();
        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() + mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;
            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }
        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator(mappedFiles.size());
        List<MappedFile> toRemoves = new ArrayList<>();
        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                toRemoves.add(mappedFileLast);
            }
        }
        if (!toRemoves.isEmpty()) {
            this.mappedFiles.removeAll(toRemoves);
        }
        return true;
    }

    /**
     * 获取最小偏移量，这里是获取第一个mappedFile文件对应的起始偏移量，而不是0，因为过期文件将会被删除
     */
    public long getMinOffset() {
        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException ignored) {

            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 获取最大偏移量
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 获取最大写指针
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    /**
     * 计算剩余多少数据待提交，等于 当前最大写指针 - 当前提交指针
     */
    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - getCommittedWhere();
    }

    /**
     * 计算剩余多少数据待刷盘，等于 当前最大偏移量 - 当前刷盘指针
     */
    public long remainHowManyDataToFlush() {
        return getMaxOffset() - this.getFlushedWhere();
    }

    /**
     * 删除最后一个mappedFile文件
     */
    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            //文件系统销毁
            lastMappedFile.destroy(1000);
            //从mappedFile队列中移除
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());
        }
    }

    /**
     * 根据过期时间删除过期文件
     */
    public int deleteExpiredFileByTime(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, final boolean cleanImmediately, final int deleteFileBatchMax) {
        Object[] mfs = this.copyMappedFiles();
        if (null == mfs)
            return 0;
        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        //待删除文件集合
        List<MappedFile> files = new ArrayList<>();
        checkSelf();
        for (int i = 0; i < mfsLength; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            //最大存活时间 = 文件最后一次修改时间 + 过期时间
            long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
            if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                if (mappedFile.destroy(intervalForcibly)) {
                    files.add(mappedFile);
                    deleteCount++;
                    if (files.size() >= deleteFileBatchMax) {
                        break;
                    }
                    if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                        try {
                            Thread.sleep(deleteFilesInterval);
                        } catch (InterruptedException ignore) {
                        }
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        deleteExpiredFile(files);
        return deleteCount;
    }

    /**
     * 根据偏移量删除过期文件
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles();
        //待删除文件
        List<MappedFile> files = new ArrayList<>();
        int deleteCount = 0;
        if (null != mfs) {
            int mfsLength = mfs.length - 1;
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset " + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) {
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        deleteExpiredFile(files);
        return deleteCount;
    }

    public void deleteExpiredFileByOffsetForTimerLog(long offset, int checkOffset, int unitSize) {
        Object[] mfs = this.copyMappedFiles();
        List<MappedFile> files = new ArrayList<>();
        if (null != mfs) {
            int mfsLength = mfs.length - 1;
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = false;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(checkOffset);
                try {
                    if (result != null) {
                        int position = result.getByteBuffer().position();
                        int size = result.getByteBuffer().getInt();
                        result.getByteBuffer().getLong();
                        int magic = result.getByteBuffer().getInt();
                        if (size == unitSize && (magic | 0xF) == 0xF) {
                            result.getByteBuffer().position(position + MixAll.UNIT_PRE_SIZE_FOR_MSG);
                            long maxOffsetPy = result.getByteBuffer().getLong();
                            destroy = maxOffsetPy < offset;
                            if (destroy) {
                                log.info("physic min commitlog offset " + offset + ", current mappedFile's max offset " + maxOffsetPy + ", delete it");
                            }
                        } else {
                            log.warn("Found error data in [{}] checkOffset:{} unitSize:{}", mappedFile.getFileName(), checkOffset, unitSize);
                        }
                    } else if (!mappedFile.isAvailable()) {
                        log.warn("Found a hanged consume queue file, attempting to delete it.");
                        destroy = true;
                    } else {
                        log.warn("this being not executed forever.");
                        break;
                    }
                } finally {
                    if (null != result) {
                        result.release();
                    }
                }
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                } else {
                    break;
                }
            }
        }
        deleteExpiredFile(files);
    }

    public long getTotalFileSize() {
        return (long) mappedFileSize * mappedFiles.size();
    }

    /**
     * 刷盘 todo
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.getFlushedWhere(), this.getFlushedWhere() == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.getFlushedWhere();
            this.setFlushedWhere(where);
            if (0 == flushLeastPages) {
                this.setStoreTimestamp(tmpTimeStamp);
            }
        }
        return result;
    }

    public synchronized boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.getCommittedWhere(), this.getCommittedWhere() == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.getCommittedWhere();
            this.setCommittedWhere(where);
        }
        return result;
    }

    /**
     * 根据偏移量找到对应的mappedFile文件
     *
     * @param offset Offset 偏移量
     * @param returnFirstOnNotFound 没有找到时是否返回第一个文件
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                //对偏移量的校验，小于第一个文件的起始偏移量，或者大于最后一个文件的最大偏移量，都不是合法偏移量
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}", offset, firstMappedFile.getFileFromOffset(), lastMappedFile.getFileFromOffset() + this.mappedFileSize, this.mappedFileSize, this.mappedFiles.size());
                } else {
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    //再次对偏移量进行校验
                    if (targetFile != null && offset >= targetFile.getFileFromOffset() && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }
                    //实在找不到，就遍历所有文件进行查找
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset() && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }
        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.setFlushedWhere(0);
        File file = new File(storePath);
        if (file.isDirectory()) {
            boolean flag = file.delete();
            log.info("flag:{}", flag);
        }
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        if (reserveNum < 3) {
            reserveNum = 3;
        }

        Object[] mfs = this.copyMappedFiles();
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceSwapIntervalMs) {
                mappedFile.swapMap();
                continue;
            }
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > normalSwapIntervalMs && mappedFile.getMappedByteBufferAccessCountSinceLastSwap() > 0) {
                mappedFile.swapMap();
            }
        }
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        int reserveNum = 3;
        Object[] mfs = this.copyMappedFiles();
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceCleanSwapIntervalMs) {
                mappedFile.cleanSwapedMap(false);
            }
        }
    }

    public Stream<MappedFile> stream() {
        return this.mappedFiles.stream();
    }

    public List<MappedFile> range(final long from, final long to) {
        Object[] mfs = copyMappedFiles();
        if (null == mfs) {
            return new ArrayList<>();
        }

        List<MappedFile> result = new ArrayList<>();
        for (Object mf : mfs) {
            MappedFile mappedFile = (MappedFile) mf;
            if (mappedFile.getFileFromOffset() + mappedFile.getFileSize() <= from) {
                continue;
            }

            if (to <= mappedFile.getFileFromOffset()) {
                break;
            }
            result.add(mappedFile);
        }

        return result;
    }
}

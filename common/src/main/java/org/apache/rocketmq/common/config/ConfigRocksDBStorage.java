package org.apache.rocketmq.common.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.rocksdb.*;
import org.rocksdb.CompactRangeOptions.BottommostLevelCompaction;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ConfigRocksDBStorage extends AbstractRocksDBStorage {

    public ConfigRocksDBStorage(final String dbPath) {
        super();
        this.dbPath = dbPath;
        this.readOnly = false;
    }

    public ConfigRocksDBStorage(final String dbPath, boolean readOnly) {
        super();
        this.dbPath = dbPath;
        this.readOnly = readOnly;
    }

    public static String getDBLogDir() {
        String rootPath = System.getProperty("user.home");
        if (StringUtils.isEmpty(rootPath)) {
            return "";
        }
        rootPath = rootPath + File.separator + "logs";
        UtilAll.ensureDirOK(rootPath);
        return rootPath + File.separator + "rocketmqlogs" + File.separator;
    }

    private void initOptions() {
        this.options = createConfigDBOptions();
        this.writeOptions = new WriteOptions();
        this.writeOptions.setSync(false);
        this.writeOptions.setDisableWAL(true);
        this.writeOptions.setNoSlowdown(true);
        this.ableWalWriteOptions = new WriteOptions();
        this.ableWalWriteOptions.setSync(false);
        this.ableWalWriteOptions.setDisableWAL(false);
        this.ableWalWriteOptions.setNoSlowdown(true);
        this.readOptions = new ReadOptions();
        this.readOptions.setPrefixSameAsStart(true);
        this.readOptions.setTotalOrderSeek(false);
        this.readOptions.setTailing(false);
        this.totalOrderReadOptions = new ReadOptions();
        this.totalOrderReadOptions.setPrefixSameAsStart(false);
        this.totalOrderReadOptions.setTotalOrderSeek(false);
        this.totalOrderReadOptions.setTailing(false);
        this.compactRangeOptions = new CompactRangeOptions();
        this.compactRangeOptions.setBottommostLevelCompaction(BottommostLevelCompaction.kForce);
        this.compactRangeOptions.setAllowWriteStall(true);
        this.compactRangeOptions.setExclusiveManualCompaction(false);
        this.compactRangeOptions.setChangeLevel(true);
        this.compactRangeOptions.setTargetLevel(-1);
        this.compactRangeOptions.setMaxSubcompactions(4);
        this.compactionOptions = new CompactionOptions();
        this.compactionOptions.setCompression(CompressionType.LZ4_COMPRESSION);
        this.compactionOptions.setMaxSubcompactions(4);
        this.compactionOptions.setOutputFileSizeLimit(4 * 1024 * 1024 * 1024L);
    }

    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);
            initOptions();
            final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            ColumnFamilyOptions defaultOptions = createConfigOptions();
            this.cfOptions.add(defaultOptions);
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            open(cfDescriptors, cfHandles);
            this.defaultCFHandle = cfHandles.get(0);
        } catch (final Exception e) {
            AbstractRocksDBStorage.LOGGER.error("postLoad Failed. {}", this.dbPath, e);
            return false;
        }
        return true;
    }

    @Override
    protected void preShutdown() {

    }

    private ColumnFamilyOptions createConfigOptions() {
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig().
                setFormatVersion(5).
                setIndexType(IndexType.kBinarySearch).
                setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch).
                setBlockSize(32 * SizeUnit.KB).
                setFilterPolicy(new BloomFilter(16, false)).
                setCacheIndexAndFilterBlocks(false).
                setCacheIndexAndFilterBlocksWithHighPriority(true).
                setPinL0FilterAndIndexBlocksInCache(false).
                setPinTopLevelIndexAndFilter(true).
                setBlockCache(new LRUCache(4 * SizeUnit.MB, 8, false)).
                setWholeKeyFiltering(true);
        ColumnFamilyOptions options = new ColumnFamilyOptions();
        return options.setMaxWriteBufferNumber(2).
                setWriteBufferSize(8 * SizeUnit.MB).
                setMinWriteBufferNumberToMerge(1).
                setTableFormatConfig(blockBasedTableConfig).
                setMemTableConfig(new SkipListMemTableConfig()).
                setCompressionType(CompressionType.NO_COMPRESSION).
                setNumLevels(7).
                setCompactionStyle(CompactionStyle.LEVEL).
                setLevel0FileNumCompactionTrigger(4).
                setLevel0SlowdownWritesTrigger(8).
                setLevel0StopWritesTrigger(12).
                setTargetFileSizeBase(64 * SizeUnit.MB).
                setTargetFileSizeMultiplier(2).
                setMaxBytesForLevelBase(256 * SizeUnit.MB).
                setMaxBytesForLevelMultiplier(2).
                setMergeOperator(new StringAppendOperator()).
                setInplaceUpdateSupport(true);
    }

    private DBOptions createConfigDBOptions() {
        DBOptions options = new DBOptions();
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        return options.setDbLogDir(getDBLogDir()).setInfoLogLevel(InfoLogLevel.INFO_LEVEL).setWalRecoveryMode(WALRecoveryMode.SkipAnyCorruptedRecords).setManualWalFlush(true).setMaxTotalWalSize(500 * SizeUnit.MB).setWalSizeLimitMB(0).setWalTtlSeconds(0).setCreateIfMissing(true).setCreateMissingColumnFamilies(true).setMaxOpenFiles(-1).setMaxLogFileSize(SizeUnit.GB).setKeepLogFileNum(5).setMaxManifestFileSize(SizeUnit.GB).
                setAllowConcurrentMemtableWrite(false).
                setStatistics(statistics).
                setStatsDumpPeriodSec(600).
                setAtomicFlush(true).
                setMaxBackgroundJobs(32).
                setMaxSubcompactions(4).
                setParanoidChecks(true).
                setDelayedWriteRate(16 * SizeUnit.MB).
                setRateLimiter(new RateLimiter(100 * SizeUnit.MB)).
                setUseDirectIoForFlushAndCompaction(true).
                setUseDirectReads(true);
    }

    public void put(final byte[] keyBytes, final int keyLen, final byte[] valueBytes) throws Exception {
        put(this.defaultCFHandle, this.ableWalWriteOptions, keyBytes, keyLen, valueBytes, valueBytes.length);
    }

    public void put(final ByteBuffer keyBB, final ByteBuffer valueBB) throws Exception {
        put(this.defaultCFHandle, this.ableWalWriteOptions, keyBB, valueBB);
    }

    public byte[] get(final byte[] keyBytes) throws Exception {
        return get(this.defaultCFHandle, this.totalOrderReadOptions, keyBytes);
    }

    public void delete(final byte[] keyBytes) throws Exception {
        delete(this.defaultCFHandle, this.ableWalWriteOptions, keyBytes);
    }

    public void batchPutWithWal(final WriteBatch batch) throws RocksDBException {
        batchPut(this.ableWalWriteOptions, batch);
    }

    public RocksIterator iterator() {
        return this.db.newIterator(this.defaultCFHandle, this.totalOrderReadOptions);
    }

    public RocksIterator iterator(ReadOptions readOptions) {
        return this.db.newIterator(this.defaultCFHandle, readOptions);
    }

}
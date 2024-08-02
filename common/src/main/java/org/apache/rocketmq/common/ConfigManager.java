package org.apache.rocketmq.common;

import org.apache.rocketmq.common.config.RocksDBConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.Statistics;

import java.io.IOException;

public abstract class ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    protected RocksDBConfigManager rocksDBConfigManager;

    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            if (null == jsonString || jsonString.isEmpty()) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath() + ".bak";
            String jsonString = MixAll.file2String(fileName);
            if (jsonString != null && !jsonString.isEmpty()) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }
        return true;
    }

    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    protected void decode0(final byte[] key, final byte[] body) {

    }

    public boolean stop() {
        return true;
    }

    public abstract String configFilePath();

    public abstract String encode();

    public abstract String encode(final boolean prettyFormat);

    public abstract void decode(final String jsonString);

    public Statistics getStatistics() {
        return rocksDBConfigManager == null ? null : rocksDBConfigManager.getStatistics();
    }

}
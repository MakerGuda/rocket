package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.store.Swappable;

/**
 * FileQueueLifeCycle contains life cycle methods of ConsumerQueue that is directly implemented by FILE.
 */
public interface FileQueueLifeCycle extends Swappable {
    /**
     * Load from file.
     *
     * @return true if loaded successfully.
     */
    boolean load();

    /**
     * Recover from file.
     */
    void recover();

    /**
     * Check files.
     */
    void checkSelf();

    /**
     * Flush cache to file.
     *
     * @param flushLeastPages the minimum number of pages to be flushed
     * @return true if any data has been flushed.
     */
    boolean flush(int flushLeastPages);

    /**
     * Destroy files.
     */
    void destroy();

    /**
     * Truncate dirty logic files starting at max commit log position.
     *
     * @param maxCommitLogPos max commit log position
     */
    void truncateDirtyLogicFiles(long maxCommitLogPos);

    /**
     * Delete expired files ending at min commit log position.
     *
     * @param minCommitLogPos min commit log position
     * @return deleted file numbers.
     */
    int deleteExpiredFile(long minCommitLogPos);

    /**
     * Roll to next file.
     *
     * @param nextBeginOffset next begin offset
     * @return the beginning offset of the next file
     */
    long rollNextFile(final long nextBeginOffset);

    /**
     * Is the first file available?
     *
     * @return true if it's available
     */
    boolean isFirstFileAvailable();

    /**
     * Does the first file exist?
     *
     * @return true if it exists
     */
    boolean isFirstFileExist();
}

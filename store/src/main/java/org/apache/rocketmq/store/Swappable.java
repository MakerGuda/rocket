package org.apache.rocketmq.store;

public interface Swappable {

    void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs);

    void cleanSwappedMap(long forceCleanSwapIntervalMs);

}
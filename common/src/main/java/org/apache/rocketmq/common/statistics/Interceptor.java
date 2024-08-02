package org.apache.rocketmq.common.statistics;

public interface Interceptor {

    void inc(long... deltas);

    void reset();

}
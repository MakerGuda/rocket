package org.apache.rocketmq.common.thread;

import java.util.concurrent.ThreadPoolExecutor;

public interface ThreadPoolStatusMonitor {

    String describe();

    double value(ThreadPoolExecutor executor);

    boolean needPrintJstack(ThreadPoolExecutor executor, double value);

}
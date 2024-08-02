package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadFactoryImpl implements ThreadFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final AtomicLong threadIndex = new AtomicLong(0);

    private final String threadNamePrefix;

    private final boolean daemon;

    public ThreadFactoryImpl(final String threadNamePrefix) {
        this(threadNamePrefix, false);
    }

    public ThreadFactoryImpl(final String threadNamePrefix, boolean daemon) {
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }

    public ThreadFactoryImpl(final String threadNamePrefix, BrokerIdentity brokerIdentity) {
        this(threadNamePrefix, false, brokerIdentity);
    }

    public ThreadFactoryImpl(final String threadNamePrefix, boolean daemon, BrokerIdentity brokerIdentity) {
        this.daemon = daemon;
        if (brokerIdentity != null && brokerIdentity.isInBrokerContainer()) {
            this.threadNamePrefix = brokerIdentity.getIdentifier() + threadNamePrefix;
        } else {
            this.threadNamePrefix = threadNamePrefix;
        }
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler((t, e) -> LOGGER.error("[BUG] Thread has an uncaught exception, threadId={}, threadName={}", t.getId(), t.getName(), e));
        return thread;
    }

}
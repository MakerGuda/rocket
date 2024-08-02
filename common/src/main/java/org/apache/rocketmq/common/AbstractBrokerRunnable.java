package org.apache.rocketmq.common;

import org.apache.rocketmq.logging.org.slf4j.MDC;

import java.io.File;

public abstract class AbstractBrokerRunnable implements Runnable {

    protected final BrokerIdentity brokerIdentity;

    public AbstractBrokerRunnable(BrokerIdentity brokerIdentity) {
        this.brokerIdentity = brokerIdentity;
    }

    private static final String MDC_BROKER_CONTAINER_LOG_DIR = "brokerContainerLogDir";

    public abstract void run0();

    @Override
    public void run() {
        try {
            if (brokerIdentity.isInBrokerContainer()) {
                MDC.put(MDC_BROKER_CONTAINER_LOG_DIR, File.separator + brokerIdentity.getCanonicalName());
            }
            run0();
        } finally {
            MDC.clear();
        }
    }

}
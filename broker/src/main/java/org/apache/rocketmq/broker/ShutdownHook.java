package org.apache.rocketmq.broker;

public interface ShutdownHook {

    void beforeShutdown(BrokerController controller);

}
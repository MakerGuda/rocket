package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;

import java.io.IOException;

public interface TraceDispatcher {

    enum Type {
        PRODUCE, CONSUME
    }

    void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException;

    boolean append(Object ctx);

    void flush() throws IOException;

    void shutdown();

}
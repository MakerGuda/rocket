package org.apache.rocketmq.client.impl;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.ProduceAccumulator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
public class MQClientManager {

    private final static Logger log = LoggerFactory.getLogger(MQClientManager.class);

    @Getter
    private static MQClientManager instance = new MQClientManager();

    private AtomicInteger factoryIndexGenerator = new AtomicInteger();

    /**
     * key: clientId 一个客户端id只会有一个 MQClientInstance
     */
    private ConcurrentMap<String, MQClientInstance> factoryTable = new ConcurrentHashMap<>();

    /**
     * key: clientId 一个客户端id只会有一个 ProduceAccumulator
     */
    private ConcurrentMap<String, ProduceAccumulator> accumulatorTable = new ConcurrentHashMap<String, ProduceAccumulator>();

    /**
     * 构造函数私有化，单例模式
     */
    private MQClientManager() {

    }

    /**
     * 获取或者创建当前客户端对应的MQClientInstance
     */
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }

    /**
     * 获取或者创建当前客户端对应的MQClientInstance
     */
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance = new MQClientInstance(clientConfig.cloneClientConfig(), this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }
        return instance;
    }

    public ProduceAccumulator getOrCreateProduceAccumulator(final ClientConfig clientConfig) {
        String clientId = clientConfig.buildMQClientId();
        ProduceAccumulator accumulator = this.accumulatorTable.get(clientId);
        if (null == accumulator) {
            accumulator = new ProduceAccumulator(clientId);
            ProduceAccumulator prev = this.accumulatorTable.putIfAbsent(clientId, accumulator);
            if (prev != null) {
                accumulator = prev;
                log.warn("Returned Previous ProduceAccumulator for clientId:[{}]", clientId);
            } else {
                log.info("Created new ProduceAccumulator for clientId:[{}]", clientId);
            }
        }
        return accumulator;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }

}
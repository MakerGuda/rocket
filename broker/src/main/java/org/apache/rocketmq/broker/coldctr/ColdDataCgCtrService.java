package org.apache.rocketmq.broker.coldctr;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.coldctr.AccAndTimeStamp;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Setter
public class ColdDataCgCtrService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_COLDCTR_LOGGER_NAME);

    private static final AtomicLong GLOBAL_ACC = new AtomicLong(0L);

    private static final String ADAPTIVE = "|adaptive";

    private final SystemClock systemClock = new SystemClock();

    private final long cgColdAccResideTimeoutMills = 60 * 1000;

    private final ConcurrentHashMap<String, AccAndTimeStamp> cgColdThresholdMapRuntime = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Long> cgColdThresholdMapConfig = new ConcurrentHashMap<>();

    private final BrokerConfig brokerConfig;

    private final MessageStoreConfig messageStoreConfig;

    private final ColdCtrStrategy coldCtrStrategy;

    public ColdDataCgCtrService(BrokerController brokerController) {
        this.brokerConfig = brokerController.getBrokerConfig();
        this.messageStoreConfig = brokerController.getMessageStoreConfig();
        this.coldCtrStrategy = brokerConfig.isUsePIDColdCtrStrategy() ? new PIDAdaptiveColdCtrStrategy(this, (long) (brokerConfig.getGlobalColdReadThreshold() * 0.8)) : new SimpleColdCtrStrategy(this);
    }

    @Override
    public String getServiceName() {
        return ColdDataCgCtrService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (messageStoreConfig.isColdDataFlowControlEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(180 * 1000);
                }
                long beginLockTimestamp = this.systemClock.now();
                clearDataAcc();
                if (!brokerConfig.isColdCtrStrategyEnable()) {
                    clearAdaptiveConfig();
                }
                long costTime = this.systemClock.now() - beginLockTimestamp;
                log.info("[{}] clearTheDataAcc-cost {} ms.", costTime > 3 * 1000 ? "NOTIFYME" : "OK", costTime);
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception", e);
            }
        }
        log.info("{} service end", this.getServiceName());
    }

    public String getColdDataFlowCtrInfo() {
        JSONObject result = new JSONObject();
        result.put("runtimeTable", this.cgColdThresholdMapRuntime);
        result.put("configTable", this.cgColdThresholdMapConfig);
        result.put("cgColdReadThreshold", this.brokerConfig.getCgColdReadThreshold());
        result.put("globalColdReadThreshold", this.brokerConfig.getGlobalColdReadThreshold());
        result.put("globalAcc", GLOBAL_ACC.get());
        return result.toJSONString();
    }

    private void clearDataAcc() {
        log.info("clearDataAcc cgColdThresholdMapRuntime key size: {}", cgColdThresholdMapRuntime.size());
        if (brokerConfig.isColdCtrStrategyEnable()) {
            coldCtrStrategy.collect(GLOBAL_ACC.get());
        }
        Iterator<Entry<String, AccAndTimeStamp>> iterator = cgColdThresholdMapRuntime.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, AccAndTimeStamp> next = iterator.next();
            if (System.currentTimeMillis() >= cgColdAccResideTimeoutMills + next.getValue().getLastColdReadTimeMills()) {
                if (brokerConfig.isColdCtrStrategyEnable()) {
                    cgColdThresholdMapConfig.remove(buildAdaptiveKey(next.getKey()));
                }
                iterator.remove();
            } else if (next.getValue().getColdAcc().get() >= getThresholdByConsumerGroup(next.getKey())) {
                log.info("Coldctr consumerGroup: {}, acc: {}, threshold: {}", next.getKey(), next.getValue().getColdAcc().get(), getThresholdByConsumerGroup(next.getKey()));
                if (brokerConfig.isColdCtrStrategyEnable() && !isGlobalColdCtr() && !isAdminConfig(next.getKey())) {
                    coldCtrStrategy.promote(buildAdaptiveKey(next.getKey()), getThresholdByConsumerGroup(next.getKey()));
                }
            }
            next.getValue().getColdAcc().set(0L);
        }
        if (isGlobalColdCtr()) {
            log.info("Coldctr global acc: {}, threshold: {}", GLOBAL_ACC.get(), this.brokerConfig.getGlobalColdReadThreshold());
        }
        if (brokerConfig.isColdCtrStrategyEnable()) {
            sortAndDecelerate();
        }
        GLOBAL_ACC.set(0L);
    }

    private void sortAndDecelerate() {
        List<Entry<String, Long>> configMapList = new ArrayList<>(cgColdThresholdMapConfig.entrySet());
        configMapList.sort((o1, o2) -> (int) (o2.getValue() - o1.getValue()));
        Iterator<Entry<String, Long>> iterator = configMapList.iterator();
        int maxDecelerate = 3;
        while (iterator.hasNext() && maxDecelerate > 0) {
            Entry<String, Long> next = iterator.next();
            if (!isAdminConfig(next.getKey())) {
                coldCtrStrategy.decelerate(next.getKey(), getThresholdByConsumerGroup(next.getKey()));
                maxDecelerate--;
            }
        }
    }

    public void coldAcc(String consumerGroup, long coldDataToAcc) {
        if (coldDataToAcc <= 0) {
            return;
        }
        GLOBAL_ACC.addAndGet(coldDataToAcc);
        AccAndTimeStamp atomicAcc = cgColdThresholdMapRuntime.get(consumerGroup);
        if (null == atomicAcc) {
            atomicAcc = new AccAndTimeStamp(new AtomicLong(coldDataToAcc));
            atomicAcc = cgColdThresholdMapRuntime.putIfAbsent(consumerGroup, atomicAcc);
        }
        if (null != atomicAcc) {
            atomicAcc.getColdAcc().addAndGet(coldDataToAcc);
            atomicAcc.setLastColdReadTimeMills(System.currentTimeMillis());
        }
    }

    public void addOrUpdateGroupConfig(String consumerGroup, Long threshold) {
        cgColdThresholdMapConfig.put(consumerGroup, threshold);
    }

    public void removeGroupConfig(String consumerGroup) {
        cgColdThresholdMapConfig.remove(consumerGroup);
    }

    public boolean isCgNeedColdDataFlowCtr(String consumerGroup) {
        if (!this.messageStoreConfig.isColdDataFlowControlEnable()) {
            return false;
        }
        if (MixAll.isSysConsumerGroupForNoColdReadLimit(consumerGroup)) {
            return false;
        }
        AccAndTimeStamp accAndTimeStamp = cgColdThresholdMapRuntime.get(consumerGroup);
        if (null == accAndTimeStamp) {
            return false;
        }
        Long threshold = getThresholdByConsumerGroup(consumerGroup);
        if (accAndTimeStamp.getColdAcc().get() >= threshold) {
            return true;
        }
        return GLOBAL_ACC.get() >= this.brokerConfig.getGlobalColdReadThreshold();
    }

    public boolean isGlobalColdCtr() {
        return GLOBAL_ACC.get() > this.brokerConfig.getGlobalColdReadThreshold();
    }

    private Long getThresholdByConsumerGroup(String consumerGroup) {
        if (isAdminConfig(consumerGroup)) {
            if (consumerGroup.endsWith(ADAPTIVE)) {
                return cgColdThresholdMapConfig.get(consumerGroup.split(ADAPTIVE)[0]);
            }
            return cgColdThresholdMapConfig.get(consumerGroup);
        }
        Long threshold = null;
        if (brokerConfig.isColdCtrStrategyEnable()) {
            if (consumerGroup.endsWith(ADAPTIVE)) {
                threshold = cgColdThresholdMapConfig.get(consumerGroup);
            } else {
                threshold = cgColdThresholdMapConfig.get(buildAdaptiveKey(consumerGroup));
            }
        }
        if (null == threshold) {
            threshold = this.brokerConfig.getCgColdReadThreshold();
        }
        return threshold;
    }

    private String buildAdaptiveKey(String consumerGroup) {
        return consumerGroup + ADAPTIVE;
    }

    private boolean isAdminConfig(String consumerGroup) {
        if (consumerGroup.endsWith(ADAPTIVE)) {
            consumerGroup = consumerGroup.split(ADAPTIVE)[0];
        }
        return cgColdThresholdMapConfig.containsKey(consumerGroup);
    }

    private void clearAdaptiveConfig() {
        cgColdThresholdMapConfig.entrySet().removeIf(next -> next.getKey().endsWith(ADAPTIVE));
    }

}
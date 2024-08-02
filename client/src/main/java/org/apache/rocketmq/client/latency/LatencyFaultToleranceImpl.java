package org.apache.rocketmq.client.latency;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {

    private final static Logger log = LoggerFactory.getLogger(MQFaultStrategy.class);

    /**
     * key: brokerName
     */
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<>(16);

    /**
     * 默认嗅探超时时间
     */
    private int detectTimeout = 200;

    /**
     * 默认嗅探时间间隔
     */
    private int detectInterval = 2000;

    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    private volatile boolean startDetectorEnable = false;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "LatencyFaultToleranceScheduledThread"));

    private final Resolver resolver;

    /**
     * 嗅探服务
     */
    private final ServiceDetector serviceDetector;

    public LatencyFaultToleranceImpl(Resolver resolver, ServiceDetector serviceDetector) {
        this.resolver = resolver;
        this.serviceDetector = serviceDetector;
    }

    /**
     * 服务嗅探
     */
    @Override
    public void detectByOneRound() {
        for (Map.Entry<String, FaultItem> item : this.faultItemTable.entrySet()) {
            FaultItem brokerItem = item.getValue();
            //当前时间大于等于下次检查时间
            if (System.currentTimeMillis() - brokerItem.checkStamp >= 0) {
                //下次嗅探时间 = 当前时间 + 嗅探时间间隔
                brokerItem.checkStamp = System.currentTimeMillis() + this.detectInterval;
                //获取brokerName对应的master地址
                String brokerAddr = resolver.resolve(brokerItem.getName());
                if (brokerAddr == null) {
                    faultItemTable.remove(item.getKey());
                    continue;
                }
                if (null == serviceDetector) {
                    continue;
                }
                //服务可用，将broker设置为可达
                boolean serviceOK = serviceDetector.detect(brokerAddr, detectTimeout);
                if (serviceOK && !brokerItem.reachableFlag) {
                    log.info(brokerItem.name + " is reachable now, then it can be used.");
                    brokerItem.reachableFlag = true;
                }
            }
        }
    }

    /**
     * 启动一个新线程，服务嗅探是否可用
     */
    @Override
    public void startDetector() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                if (startDetectorEnable) {
                    detectByOneRound();
                }
            } catch (Exception e) {
                log.warn("Unexpected exception raised while detecting service reachability", e);
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    /**
     * 关闭线程池
     */
    @Override
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration, final boolean reachable) {
        FaultItem old = this.faultItemTable.get(name);
        if (null == old) {
            final FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            faultItem.updateNotAvailableDuration(notAvailableDuration);
            faultItem.setReachableFlag(reachable);
            old = this.faultItemTable.putIfAbsent(name, faultItem);
        }
        if (null != old) {
            old.setCurrentLatency(currentLatency);
            old.updateNotAvailableDuration(notAvailableDuration);
            old.setReachableFlag(reachable);
        }
        if (!reachable) {
            log.info(name + " is unreachable, it will not be used until it's reachable");
        }
    }

    /**
     * 判断当前broker是否可用
     */
    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isAvailable();
        }
        return true;
    }

    /**
     * 判断当前broker是否可达
     */
    @Override
    public boolean isReachable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isReachableFlag();
        }
        return true;
    }

    /**
     * 将broker从容错表中移除
     */
    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    /**
     * 设置是否启动嗅探
     */
    @Override
    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" + "faultItemTable=" + faultItemTable + ", whichItemWorst=" + whichItemWorst + '}';
    }

    /**
     * 设置嗅探超时时间
     */
    @Override
    public void setDetectTimeout(final int detectTimeout) {
        this.detectTimeout = detectTimeout;
    }

    /**
     * 设置嗅探时间间隔
     */
    @Override
    public void setDetectInterval(final int detectInterval) {
        this.detectInterval = detectInterval;
    }

    @Getter
    @Setter
    public static class FaultItem implements Comparable<FaultItem> {

        private final String name;

        private volatile long currentLatency;

        private volatile long startTimestamp;

        private volatile long checkStamp;

        private volatile boolean reachableFlag;

        public FaultItem(final String name) {
            this.name = name;
        }

        /**
         * 更新不可用时间，将开始可用时间设置为 当前时间 + 不可用时长
         */
        public void updateNotAvailableDuration(long notAvailableDuration) {
            if (notAvailableDuration > 0 && System.currentTimeMillis() + notAvailableDuration > this.startTimestamp) {
                this.startTimestamp = System.currentTimeMillis() + notAvailableDuration;
                log.info(name + " will be isolated for " + notAvailableDuration + " ms.");
            }
        }

        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable()) {
                    return -1;
                }
                if (other.isAvailable()) {
                    return 1;
                }
            }
            if (this.currentLatency < other.currentLatency) {
                return -1;
            } else if (this.currentLatency > other.currentLatency) {
                return 1;
            }
            if (this.startTimestamp < other.startTimestamp) {
                return -1;
            } else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }
            return 0;
        }

        /**
         * 判断当前时间是否大于等于可用开始时间
         */
        public boolean isAvailable() {
            return System.currentTimeMillis() >= startTimestamp;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FaultItem)) {
                return false;
            }
            final FaultItem faultItem = (FaultItem) o;
            if (getCurrentLatency() != faultItem.getCurrentLatency()) {
                return false;
            }
            if (getStartTimestamp() != faultItem.getStartTimestamp()) {
                return false;
            }
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;
        }

        @Override
        public String toString() {
            return "FaultItem{" + "name='" + name + '\'' + ", currentLatency=" + currentLatency + ", startTimestamp=" + startTimestamp + ", reachableFlag=" + reachableFlag + '}';
        }

    }

}
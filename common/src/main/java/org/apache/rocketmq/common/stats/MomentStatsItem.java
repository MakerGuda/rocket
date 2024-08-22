package org.apache.rocketmq.common.stats;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Setter
public class MomentStatsItem {

    private final AtomicLong value = new AtomicLong(0);

    private final String statsName;

    private final String statsKey;

    private final ScheduledExecutorService scheduledExecutorService;

    private final Logger log;

    public MomentStatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, Logger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }

    public void init() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtMinutes();
                MomentStatsItem.this.value.set(0);
            } catch (Throwable ignore) {
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }

    public void printAtMinutes() {
        log.info("[{}] [{}] Stats Every 5 Minutes, Value: {}", this.statsName, this.statsKey, this.value.get());
    }

}
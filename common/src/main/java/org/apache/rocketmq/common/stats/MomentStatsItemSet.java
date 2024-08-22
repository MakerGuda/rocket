package org.apache.rocketmq.common.stats;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class MomentStatsItemSet {

    private final ConcurrentMap<String, MomentStatsItem> statsItemTable = new ConcurrentHashMap<>(128);

    private final String statsName;

    private final ScheduledExecutorService scheduledExecutorService;

    private final Logger log;

    public MomentStatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, Logger log) {
        this.statsName = statsName;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
        this.init();
    }

    public void init() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtMinutes();
            } catch (Throwable ignore) {
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }

    private void printAtMinutes() {
        for (Entry<String, MomentStatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().printAtMinutes();
        }
    }

    public void delValueByInfixKey(final String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().contains(separator + statsKey + separator));
    }

    public void delValueBySuffixKey(final String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().endsWith(separator + statsKey));
    }

    public MomentStatsItem getAndCreateStatsItem(final String statsKey) {
        MomentStatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            statsItem = new MomentStatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            MomentStatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);
            if (null != prev) {
                statsItem = prev;
            }
        }
        return statsItem;
    }

}
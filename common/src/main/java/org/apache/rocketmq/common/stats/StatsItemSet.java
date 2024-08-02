package org.apache.rocketmq.common.stats;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StatsItemSet {

    private final ConcurrentMap<String, StatsItem> statsItemTable = new ConcurrentHashMap<>(128);

    private final String statsName;

    private final ScheduledExecutorService scheduledExecutorService;

    private final Logger logger;

    public StatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, Logger logger) {
        this.logger = logger;
        this.statsName = statsName;
        this.scheduledExecutorService = scheduledExecutorService;
        this.init();
    }

    public void init() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                samplingInSeconds();
            } catch (Throwable ignored) {
            }
        }, 0, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                samplingInMinutes();
            } catch (Throwable ignored) {
            }
        }, 0, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                samplingInHour();
            } catch (Throwable ignored) {
            }
        }, 0, 1, TimeUnit.HOURS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtMinutes();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtHour();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextHourTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtDay();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    private void samplingInSeconds() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().samplingInSeconds();
        }
    }

    private void samplingInMinutes() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().samplingInMinutes();
        }
    }

    private void samplingInHour() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().samplingInHour();
        }
    }

    private void printAtMinutes() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().printAtMinutes();
        }
    }

    private void printAtHour() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().printAtHour();
        }
    }

    private void printAtDay() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().printAtDay();
        }
    }

    public void addValue(final String statsKey, final int incValue, final int incTimes) {
        StatsItem statsItem = this.getAndCreateStatsItem(statsKey);
        statsItem.getValue().add(incValue);
        statsItem.getTimes().add(incTimes);
    }

    public void addRTValue(final String statsKey, final int incValue, final int incTimes) {
        StatsItem statsItem = this.getAndCreateRTStatsItem(statsKey);
        statsItem.getValue().add(incValue);
        statsItem.getTimes().add(incTimes);
    }

    public void delValue(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            this.statsItemTable.remove(statsKey);
        }
    }

    public void delValueByPrefixKey(final String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().startsWith(statsKey + separator));
    }

    public void delValueByInfixKey(final String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().contains(separator + statsKey + separator));
    }

    public void delValueBySuffixKey(final String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().endsWith(separator + statsKey));
    }

    public StatsItem getAndCreateStatsItem(final String statsKey) {
        return getAndCreateItem(statsKey, false);
    }

    public StatsItem getAndCreateRTStatsItem(final String statsKey) {
        return getAndCreateItem(statsKey, true);
    }

    public StatsItem getAndCreateItem(final String statsKey, boolean rtItem) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            if (rtItem) {
                statsItem = new RTStatsItem(this.statsName, statsKey, this.scheduledExecutorService, logger);
            } else {
                statsItem = new StatsItem(this.statsName, statsKey, this.scheduledExecutorService, logger);
            }
            StatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);
            if (null != prev) {
                statsItem = prev;
            }
        }
        return statsItem;
    }

    public StatsSnapshot getStatsDataInMinute(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInMinute();
        }
        return new StatsSnapshot();
    }

    public StatsSnapshot getStatsDataInHour(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInHour();
        }
        return new StatsSnapshot();
    }

    public StatsItem getStatsItem(final String statsKey) {
        return this.statsItemTable.get(statsKey);
    }

}
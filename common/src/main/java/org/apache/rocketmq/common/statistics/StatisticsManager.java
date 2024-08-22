package org.apache.rocketmq.common.statistics;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.utils.ThreadUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class StatisticsManager {

    private static final int MAX_IDLE_TIME = 10 * 60 * 1000;

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> statsTable = new ConcurrentHashMap<>();

    private final ScheduledExecutorService executor = ThreadUtils.newSingleThreadScheduledExecutor("StatisticsManagerCleaner", true);

    private Map<String, StatisticsKindMeta> kindMetaMap;

    private Pair<String, long[][]>[] briefMetas;

    private StatisticsItemStateGetter statisticsItemStateGetter;

    public StatisticsManager() {
        kindMetaMap = new HashMap<>();
        start();
    }

    public void addStatisticsKindMeta(StatisticsKindMeta kindMeta) {
        kindMetaMap.put(kindMeta.getName(), kindMeta);
        statsTable.putIfAbsent(kindMeta.getName(), new ConcurrentHashMap<>(16));
    }

    public void setBriefMeta(Pair<String, long[][]>[] briefMetas) {
        this.briefMetas = briefMetas;
    }

    private void start() {
        int maxIdleTime = MAX_IDLE_TIME;
        executor.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, ConcurrentHashMap<String, StatisticsItem>> entry : statsTable.entrySet()) {
                ConcurrentHashMap<String, StatisticsItem> itemMap = entry.getValue();
                if (itemMap == null || itemMap.isEmpty()) {
                    continue;
                }
                HashMap<String, StatisticsItem> tmpItemMap = new HashMap<>(itemMap);
                for (StatisticsItem item : tmpItemMap.values()) {
                    if (System.currentTimeMillis() - item.getLastTimeStamp().get() > MAX_IDLE_TIME && (statisticsItemStateGetter == null || !statisticsItemStateGetter.online(item))) {
                        remove(item);
                    }
                }
            }
        }, maxIdleTime, maxIdleTime / 3, TimeUnit.MILLISECONDS);
    }

    public void inc(String kind, String key, long... itemAccumulates) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = statsTable.get(kind);
        if (itemMap != null) {
            StatisticsItem item = itemMap.get(key);
            if (item == null) {
                item = new StatisticsItem(kind, key, kindMetaMap.get(kind).getItemNames());
                item.setInterceptor(new StatisticsBriefInterceptor(item, briefMetas));
                StatisticsItem oldItem = itemMap.putIfAbsent(key, item);
                if (oldItem != null) {
                    item = oldItem;
                } else {
                    scheduleStatisticsItem(item);
                }
            }
            item.incItems(itemAccumulates);
        }
    }

    private void scheduleStatisticsItem(StatisticsItem item) {
        kindMetaMap.get(item.getStatKind()).getScheduledPrinter().schedule(item);
    }

    public void remove(StatisticsItem item) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = statsTable.get(item.getStatKind());
        if (itemMap != null) {
            itemMap.remove(item.getStatObject(), item);
        }
        StatisticsKindMeta kindMeta = kindMetaMap.get(item.getStatKind());
        if (kindMeta != null && kindMeta.getScheduledPrinter() != null) {
            kindMeta.getScheduledPrinter().remove(item);
        }
    }

}
package org.apache.rocketmq.common.statistics;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class StatisticsItemScheduledIncrementPrinter extends StatisticsItemScheduledPrinter {

    public static final int TPS_INITIAL_DELAY = 0;

    public static final int TPS_INTREVAL = 1000;

    public static final String SEPARATOR = "|";

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> lastItemSnapshots = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItemSampleBrief>> sampleBriefs = new ConcurrentHashMap<>();

    private String[] tpsItemNames;

    public StatisticsItemScheduledIncrementPrinter(String name, StatisticsItemPrinter printer, ScheduledExecutorService executor, InitialDelay initialDelay, long interval, String[] tpsItemNames, Valve valve) {
        super(name, printer, executor, initialDelay, interval, valve);
        this.tpsItemNames = tpsItemNames;
    }

    @Override
    public void schedule(final StatisticsItem item) {
        setItemSampleBrief(item.getStatKind(), item.getStatObject(), new StatisticsItemSampleBrief(item, tpsItemNames));
        ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
            if (!enabled()) {
                return;
            }
            StatisticsItem snapshot = item.snapshot();
            StatisticsItem lastSnapshot = getItemSnapshot(lastItemSnapshots, item.getStatKind(), item.getStatObject());
            StatisticsItem increment = snapshot.subtract(lastSnapshot);
            Interceptor interceptor = item.getInterceptor();
            String interceptorStr = formatInterceptor(interceptor);
            if (interceptor != null) {
                interceptor.reset();
            }
            StatisticsItemSampleBrief brief = getSampleBrief(item.getStatKind(), item.getStatObject());
            if (brief != null && (!increment.allZeros() || printZeroLine())) {
                printer.print(name, increment, interceptorStr, brief.toString());
            }
            setItemSnapshot(lastItemSnapshots, snapshot);
            if (brief != null) {
                brief.reset();
            }
        }, getInitialDelay(), interval, TimeUnit.MILLISECONDS);
        addFuture(item, future);
        ScheduledFuture<?> futureSample = executor.scheduleAtFixedRate(() -> {
            if (!enabled()) {
                return;
            }
            StatisticsItem snapshot = item.snapshot();
            StatisticsItemSampleBrief brief = getSampleBrief(item.getStatKind(), item.getStatObject());
            if (brief != null) {
                brief.sample(snapshot);
            }
        }, TPS_INTREVAL, TPS_INTREVAL, TimeUnit.MILLISECONDS);
        addFuture(item, futureSample);
    }

    @Override
    public void remove(StatisticsItem item) {
        removeAllFuture(item);
        String kind = item.getStatKind();
        String key = item.getStatObject();
        ConcurrentHashMap<String, StatisticsItem> lastItemMap = lastItemSnapshots.get(kind);
        if (lastItemMap != null) {
            lastItemMap.remove(key);
        }
        ConcurrentHashMap<String, StatisticsItemSampleBrief> briefMap = sampleBriefs.get(kind);
        if (briefMap != null) {
            briefMap.remove(key);
        }
    }

    private StatisticsItem getItemSnapshot(ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> snapshots, String kind, String key) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = snapshots.get(kind);
        return (itemMap != null) ? itemMap.get(key) : null;
    }

    private StatisticsItemSampleBrief getSampleBrief(String kind, String key) {
        ConcurrentHashMap<String, StatisticsItemSampleBrief> itemMap = sampleBriefs.get(kind);
        return (itemMap != null) ? itemMap.get(key) : null;
    }

    private void setItemSnapshot(ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> snapshots, StatisticsItem item) {
        String kind = item.getStatKind();
        String key = item.getStatObject();
        ConcurrentHashMap<String, StatisticsItem> itemMap = snapshots.get(kind);
        if (itemMap == null) {
            itemMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, StatisticsItem> oldItemMap = snapshots.putIfAbsent(kind, itemMap);
            if (oldItemMap != null) {
                itemMap = oldItemMap;
            }
        }
        itemMap.put(key, item);
    }

    private void setItemSampleBrief(String kind, String key, StatisticsItemSampleBrief brief) {
        ConcurrentHashMap<String, StatisticsItemSampleBrief> itemMap = sampleBriefs.get(kind);
        if (itemMap == null) {
            itemMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, StatisticsItemSampleBrief> oldItemMap = sampleBriefs.putIfAbsent(kind, itemMap);
            if (oldItemMap != null) {
                itemMap = oldItemMap;
            }
        }
        itemMap.put(key, brief);
    }

    private String formatInterceptor(Interceptor interceptor) {
        if (interceptor == null) {
            return "";
        }
        if (interceptor instanceof StatisticsBriefInterceptor) {
            StringBuilder sb = new StringBuilder();
            StatisticsBriefInterceptor briefInterceptor = (StatisticsBriefInterceptor) interceptor;
            for (StatisticsBrief brief : briefInterceptor.getStatisticsBriefs()) {
                long max = brief.getMax();
                long tp999 = Math.min(brief.tp999(), max);
                sb.append(SEPARATOR).append(max);
                sb.append(SEPARATOR).append(String.format("%.2f", brief.getAvg()));
                sb.append(SEPARATOR).append(tp999);
            }
            return sb.toString();
        }
        return "";
    }

    public static class StatisticsItemSampleBrief {

        public String[] itemNames;

        public ItemSampleBrief[] briefs;

        private StatisticsItem lastSnapshot;

        public StatisticsItemSampleBrief(StatisticsItem statItem, String[] itemNames) {
            this.lastSnapshot = statItem.snapshot();
            this.itemNames = itemNames;
            this.briefs = new ItemSampleBrief[itemNames.length];
            for (int i = 0; i < itemNames.length; i++) {
                this.briefs[i] = new ItemSampleBrief();
            }
        }

        public synchronized void reset() {
            for (ItemSampleBrief brief : briefs) {
                brief.reset();
            }
        }

        public synchronized void sample(StatisticsItem snapshot) {
            if (snapshot == null) {
                return;
            }
            for (int i = 0; i < itemNames.length; i++) {
                String name = itemNames[i];
                long lastValue = lastSnapshot != null ? lastSnapshot.getItemAccumulate(name).get() : 0;
                long increment = snapshot.getItemAccumulate(name).get() - lastValue;
                briefs[i].sample(increment);
            }
            lastSnapshot = snapshot;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (ItemSampleBrief brief : briefs) {
                sb.append(SEPARATOR).append(brief.max);
                sb.append(SEPARATOR).append(String.format("%.2f", brief.getAvg()));
            }
            return sb.toString();
        }
    }

    public static class ItemSampleBrief {

        private long max;

        private long min;

        private long total;

        private long cnt;

        public ItemSampleBrief() {
            reset();
        }

        public void sample(long value) {
            max = Math.max(max, value);
            min = Math.min(min, value);
            total += value;
            cnt++;
        }

        public void reset() {
            max = 0;
            min = Long.MAX_VALUE;
            total = 0;
            cnt = 0;
        }

        public long getMin() {
            return cnt > 0 ? min : 0;
        }

        public double getAvg() {
            return cnt != 0 ? ((double) total) / cnt : 0;
        }
    }

}
package org.apache.rocketmq.common.statistics;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class StatisticsItemScheduledPrinter extends FutureHolder {

    protected String name;

    protected StatisticsItemPrinter printer;

    protected ScheduledExecutorService executor;

    protected long interval;

    protected InitialDelay initialDelay;

    protected Valve valve;

    public StatisticsItemScheduledPrinter(String name, StatisticsItemPrinter printer, ScheduledExecutorService executor, InitialDelay initialDelay, long interval, Valve valve) {
        this.name = name;
        this.printer = printer;
        this.executor = executor;
        this.initialDelay = initialDelay;
        this.interval = interval;
        this.valve = valve;
    }

    public void schedule(final StatisticsItem statisticsItem) {
        ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
            if (enabled()) {
                printer.print(name, statisticsItem);
            }
        }, getInitialDelay(), interval, TimeUnit.MILLISECONDS);
        addFuture(statisticsItem, future);
    }

    public void remove(final StatisticsItem statisticsItem) {
        removeAllFuture(statisticsItem);
    }

    protected long getInitialDelay() {
        return initialDelay != null ? initialDelay.get() : 0;
    }

    protected boolean enabled() {
        return valve != null && valve.enabled();
    }

    protected boolean printZeroLine() {
        return valve != null && valve.printZeroLine();
    }

    public interface InitialDelay {

        long get();

    }

    public interface Valve {

        boolean enabled();

        boolean printZeroLine();

    }

}
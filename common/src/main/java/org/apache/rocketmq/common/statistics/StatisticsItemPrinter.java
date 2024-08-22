package org.apache.rocketmq.common.statistics;

import org.apache.rocketmq.logging.org.slf4j.Logger;

public class StatisticsItemPrinter {

    private Logger log;

    private StatisticsItemFormatter formatter;

    public StatisticsItemPrinter(StatisticsItemFormatter formatter, Logger log) {
        this.formatter = formatter;
        this.log = log;
    }

    public void log(Logger log) {
        this.log = log;
    }

    public void formatter(StatisticsItemFormatter formatter) {
        this.formatter = formatter;
    }

    public void print(String prefix, StatisticsItem statItem, String... suffixs) {
        StringBuilder suffix = new StringBuilder();
        for (String str : suffixs) {
            suffix.append(str);
        }
        if (log != null) {
            log.info("{}{}{}", prefix, formatter.format(statItem), suffix.toString());
        }
    }

}
package org.apache.rocketmq.common.stats;

import org.apache.rocketmq.logging.org.slf4j.Logger;

import java.util.concurrent.ScheduledExecutorService;

public class RTStatsItem extends StatsItem {

    public RTStatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, Logger logger) {
        super(statsName, statsKey, scheduledExecutorService, logger);
    }

    @Override
    protected String statPrintDetail(StatsSnapshot ss) {
        return String.format("TIMES: %d AVG RT: %.2f", ss.getTimes(), ss.getAvgpt());
    }

}
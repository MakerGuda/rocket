package org.apache.rocketmq.common.statistics;

import java.util.concurrent.atomic.AtomicLong;

public class StatisticsItemFormatter {

    public String format(StatisticsItem statItem) {
        final String separator = "|";
        StringBuilder sb = new StringBuilder();
        sb.append(statItem.getStatKind()).append(separator);
        sb.append(statItem.getStatObject()).append(separator);
        for (AtomicLong acc : statItem.getItemAccumulates()) {
            sb.append(acc.get()).append(separator);
        }
        sb.append(statItem.getInvokeTimes());
        return sb.toString();
    }

}
package org.apache.rocketmq.common.statistics;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StatisticsKindMeta {

    private String name;

    private String[] itemNames;

    private StatisticsItemScheduledPrinter scheduledPrinter;

}
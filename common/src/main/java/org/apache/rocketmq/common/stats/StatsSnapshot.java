package org.apache.rocketmq.common.stats;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StatsSnapshot {

    private long sum;

    private double tps;

    private long times;

    private double avgpt;

}
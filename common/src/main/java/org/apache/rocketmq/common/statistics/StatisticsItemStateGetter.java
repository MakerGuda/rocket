package org.apache.rocketmq.common.statistics;

public interface StatisticsItemStateGetter {

    boolean online(StatisticsItem item);

}
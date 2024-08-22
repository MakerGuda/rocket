package org.apache.rocketmq.common.statistics;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

@Getter
@Setter
public class StatisticsBriefInterceptor implements Interceptor {

    private int[] indexOfItems;

    private StatisticsBrief[] statisticsBriefs;

    public StatisticsBriefInterceptor(StatisticsItem item, Pair<String, long[][]>[] briefMetas) {
        indexOfItems = new int[briefMetas.length];
        statisticsBriefs = new StatisticsBrief[briefMetas.length];
        for (int i = 0; i < briefMetas.length; i++) {
            String name = briefMetas[i].getKey();
            int index = ArrayUtils.indexOf(item.getItemNames(), name);
            if (index < 0) {
                throw new IllegalArgumentException("illegal brief ItemName: " + name);
            }
            indexOfItems[i] = index;
            statisticsBriefs[i] = new StatisticsBrief(briefMetas[i].getValue());
        }
    }

    @Override
    public void inc(long... itemValues) {
        for (int i = 0; i < indexOfItems.length; i++) {
            int indexOfItem = indexOfItems[i];
            if (indexOfItem < itemValues.length) {
                statisticsBriefs[i].sample(itemValues[indexOfItem]);
            }
        }
    }

    @Override
    public void reset() {
        for (StatisticsBrief brief : statisticsBriefs) {
            brief.reset();
        }
    }

}
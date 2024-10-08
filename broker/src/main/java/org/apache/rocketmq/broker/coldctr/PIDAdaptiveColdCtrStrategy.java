package org.apache.rocketmq.broker.coldctr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PIDAdaptiveColdCtrStrategy implements ColdCtrStrategy {

    private static final int MAX_STORE_NUMS = 10;

    private static final Double KP = 0.5, KI = 0.3, KD = 0.2;

    private final List<Long> historyEtValList = new ArrayList<>();

    private final ColdDataCgCtrService coldDataCgCtrService;

    private final Long expectGlobalVal;

    private long et = 0L;

    public PIDAdaptiveColdCtrStrategy(ColdDataCgCtrService coldDataCgCtrService, Long expectGlobalVal) {
        this.coldDataCgCtrService = coldDataCgCtrService;
        this.expectGlobalVal = expectGlobalVal;
    }

    @Override
    public Double decisionFactor() {
        if (historyEtValList.size() < MAX_STORE_NUMS) {
            return 0.0;
        }
        Long et1 = historyEtValList.get(historyEtValList.size() - 1);
        Long et2 = historyEtValList.get(historyEtValList.size() - 2);
        Long differential = et1 - et2;
        Double integration = 0.0;
        for (Long item : historyEtValList) {
            integration += item;
        }
        return KP * et + KI * integration + KD * differential;
    }

    @Override
    public void promote(String consumerGroup, Long currentThreshold) {
        if (decisionFactor() > 0) {
            coldDataCgCtrService.addOrUpdateGroupConfig(consumerGroup, (long) (currentThreshold * 1.5));
        }
    }

    @Override
    public void decelerate(String consumerGroup, Long currentThreshold) {
        if (decisionFactor() < 0) {
            long changedThresholdVal = (long) (currentThreshold * 0.8);
            if (changedThresholdVal < coldDataCgCtrService.getBrokerConfig().getCgColdReadThreshold()) {
                changedThresholdVal = coldDataCgCtrService.getBrokerConfig().getCgColdReadThreshold();
            }
            coldDataCgCtrService.addOrUpdateGroupConfig(consumerGroup, changedThresholdVal);
        }
    }

    @Override
    public void collect(Long globalAcc) {
        et = expectGlobalVal - globalAcc;
        historyEtValList.add(et);
        Iterator<Long> iterator = historyEtValList.iterator();
        while (historyEtValList.size() > MAX_STORE_NUMS && iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
    }

}
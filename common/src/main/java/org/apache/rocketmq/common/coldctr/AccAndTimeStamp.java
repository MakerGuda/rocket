package org.apache.rocketmq.common.coldctr;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicLong;

@Getter
@Setter
public class AccAndTimeStamp {

    public AtomicLong coldAcc;

    public Long lastColdReadTimeMills = System.currentTimeMillis();

    public Long createTimeMills = System.currentTimeMillis();

    public AccAndTimeStamp(AtomicLong coldAcc) {
        this.coldAcc = coldAcc;
    }

}
package org.apache.rocketmq.client.impl;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FindBrokerResult {

    private final String brokerAddr;

    private final boolean slave;

    private final int brokerVersion;

    public FindBrokerResult(String brokerAddr, boolean slave, int brokerVersion) {
        this.brokerAddr = brokerAddr;
        this.slave = slave;
        this.brokerVersion = brokerVersion;
    }

}
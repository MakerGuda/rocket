package org.apache.rocketmq.tools.monitor;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.MixAll;

@Getter
@Setter
public class MonitorConfig {

    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));

    private int roundInterval = 1000 * 60;

}
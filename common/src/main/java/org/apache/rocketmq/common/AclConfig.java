package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class AclConfig {

    private List<String> globalWhiteAddrs;

    private List<PlainAccessConfig> plainAccessConfigs;

}
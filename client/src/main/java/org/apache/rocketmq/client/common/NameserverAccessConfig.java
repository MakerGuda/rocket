package org.apache.rocketmq.client.common;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NameserverAccessConfig {

    private String namesrvAddr;

    private String namesrvDomain;

    private String namesrvDomainSubgroup;

    public NameserverAccessConfig(String namesrvAddr, String namesrvDomain, String namesrvDomainSubgroup) {
        this.namesrvAddr = namesrvAddr;
        this.namesrvDomain = namesrvDomain;
        this.namesrvDomainSubgroup = namesrvDomainSubgroup;
    }

}
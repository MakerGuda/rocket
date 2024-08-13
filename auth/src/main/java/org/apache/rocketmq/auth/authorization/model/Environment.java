package org.apache.rocketmq.auth.authorization.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.utils.IPAddressUtils;

import java.util.Collections;
import java.util.List;

@Getter
@Setter
public class Environment {

    private List<String> sourceIps;

    public static Environment of(String sourceIp) {
        if (StringUtils.isEmpty(sourceIp)) {
            return null;
        }
        return of(Collections.singletonList(sourceIp));
    }

    public static Environment of(List<String> sourceIps) {
        if (CollectionUtils.isEmpty(sourceIps)) {
            return null;
        }
        Environment environment = new Environment();
        environment.setSourceIps(sourceIps);
        return environment;
    }

    public boolean isMatch(Environment environment) {
        if (CollectionUtils.isEmpty(this.sourceIps)) {
            return true;
        }
        if (CollectionUtils.isEmpty(environment.getSourceIps())) {
            return false;
        }
        String targetIp = environment.getSourceIps().get(0);
        for (String sourceIp : this.sourceIps) {
            if (IPAddressUtils.isIPInRange(targetIp, sourceIp)) {
                return true;
            }
        }
        return false;
    }

}
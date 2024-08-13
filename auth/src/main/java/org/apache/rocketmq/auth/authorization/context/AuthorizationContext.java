package org.apache.rocketmq.auth.authorization.context;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public abstract class AuthorizationContext {

    private String channelId;

    private String rpcCode;

    private Map<String, Object> extInfo;

}
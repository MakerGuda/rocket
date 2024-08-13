package org.apache.rocketmq.auth.authentication.context;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public abstract class AuthenticationContext {

    private String channelId;

    private String rpcCode;

    private Map<String, Object> extInfo;

}
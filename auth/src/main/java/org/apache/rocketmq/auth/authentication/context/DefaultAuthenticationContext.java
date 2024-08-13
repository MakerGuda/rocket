package org.apache.rocketmq.auth.authentication.context;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DefaultAuthenticationContext extends AuthenticationContext {

    private String username;

    private byte[] content;

    private String signature;

}
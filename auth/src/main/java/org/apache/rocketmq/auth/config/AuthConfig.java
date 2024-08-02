package org.apache.rocketmq.auth.config;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AuthConfig implements Cloneable {

    private String configName;

    private String clusterName;

    private String authConfigPath;

    private boolean authenticationEnabled = false;

    private String authenticationProvider;

    private String authenticationMetadataProvider;

    private String authenticationStrategy;

    private String authenticationWhitelist;

    private String initAuthenticationUser;

    private String innerClientAuthenticationCredentials;

    private boolean authorizationEnabled = false;

    private String authorizationProvider;

    private String authorizationMetadataProvider;

    private String authorizationStrategy;

    private String authorizationWhitelist;

    private boolean migrateAuthFromV1Enabled = false;

    private int userCacheMaxNum = 1000;

    private int userCacheExpiredSecond = 600;

    private int userCacheRefreshSecond = 60;

    private int aclCacheMaxNum = 1000;

    private int aclCacheExpiredSecond = 600;

    private int aclCacheRefreshSecond = 60;

    private int statefulAuthenticationCacheMaxNum = 10000;

    private int statefulAuthenticationCacheExpiredSecond = 60;

    private int statefulAuthorizationCacheMaxNum = 10000;

    private int statefulAuthorizationCacheExpiredSecond = 60;

    @Override
    public AuthConfig clone() {
        try {
            return (AuthConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

}
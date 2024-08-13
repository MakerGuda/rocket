package org.apache.rocketmq.acl;

public interface PermissionChecker {

    void check(AccessResource checkedAccess, AccessResource ownedAccess);

}
package org.apache.rocketmq.acl;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.rocketmq.acl.common.AuthenticationHeader;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;

public interface AccessValidator {

    /**
     * Parse to get the AccessResource(user, resource, needed permission)
     */
    AccessResource parse(RemotingCommand request, String remoteAddr);

    /**
     * Parse to get the AccessResource from gRPC protocol
     */
    AccessResource parse(GeneratedMessageV3 messageV3, AuthenticationHeader header);

    /**
     * Validate the access resource.
     */
    void validate(AccessResource accessResource);

    /**
     * Update the access resource config
     */
    boolean updateAccessConfig(PlainAccessConfig plainAccessConfig);

    /**
     * Delete the access resource config
     */
    boolean deleteAccessConfig(String accessKey);

    /**
     * Get the access resource config version information
     *
     */
    String getAclConfigVersion();

    boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String aclFileFullPath);

}
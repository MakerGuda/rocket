package org.apache.rocketmq.acl.plain;

import com.google.protobuf.GeneratedMessageV3;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.common.AuthenticationHeader;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;

@Getter
@Setter
public class PlainAccessValidator implements AccessValidator {

    private PlainPermissionManager aclPlugEngine;

    public PlainAccessValidator() {
        aclPlugEngine = new PlainPermissionManager();
    }

    @Override
    public AccessResource parse(RemotingCommand request, String remoteAddr) {
        return PlainAccessResource.parse(request, remoteAddr);
    }

    @Override
    public AccessResource parse(GeneratedMessageV3 messageV3, AuthenticationHeader header) {
        return PlainAccessResource.parse(messageV3, header);
    }

    @Override
    public void validate(AccessResource accessResource) {
        aclPlugEngine.validate((PlainAccessResource) accessResource);
    }

    @Override
    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {
        return aclPlugEngine.updateAccessConfig(plainAccessConfig);
    }

    @Override
    public boolean deleteAccessConfig(String accessKey) {
        return aclPlugEngine.deleteAccessConfig(accessKey);
    }

    @Override
    public String getAclConfigVersion() {
        return aclPlugEngine.getAclConfigDataVersion();
    }

    @Override
    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String aclFileFullPath) {
        return aclPlugEngine.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList, aclFileFullPath);
    }

}
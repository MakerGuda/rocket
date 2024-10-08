package org.apache.rocketmq.auth.authorization.manager;

import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Resource;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface AuthorizationMetadataManager {

    void shutdown();

    CompletableFuture<Void> createAcl(Acl acl);

    CompletableFuture<Void> updateAcl(Acl acl);

    CompletableFuture<Void> deleteAcl(Subject subject, PolicyType policyType, Resource resource);

    CompletableFuture<Acl> getAcl(Subject subject);

    CompletableFuture<List<Acl>> listAcl(String subjectFilter, String resourceFilter);

}
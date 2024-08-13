package org.apache.rocketmq.auth.authorization.provider;

import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.config.AuthConfig;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface AuthorizationMetadataProvider {

    void initialize(AuthConfig authConfig, Supplier<?> metadataService);

    void shutdown();

    CompletableFuture<Void> createAcl(Acl acl);

    CompletableFuture<Void> deleteAcl(Subject subject);

    CompletableFuture<Void> updateAcl(Acl acl);

    CompletableFuture<Acl> getAcl(Subject subject);

    CompletableFuture<List<Acl>> listAcl(String subjectFilter, String resourceFilter);

}
package org.apache.rocketmq.auth.authentication.provider;

import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.config.AuthConfig;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface AuthenticationMetadataProvider {

    void initialize(AuthConfig authConfig, Supplier<?> metadataService);

    void shutdown();

    CompletableFuture<Void> createUser(User user);

    CompletableFuture<Void> deleteUser(String username);

    CompletableFuture<Void> updateUser(User user);

    CompletableFuture<User> getUser(String username);

    CompletableFuture<List<User>> listUser(String filter);

}
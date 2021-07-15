package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.messages.Gateway;
import io.github.alikelleci.easysourcing.messages.Metadata;
import lombok.SneakyThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface CommandGateway extends Gateway {

  void sendAndForget(Object payload, Metadata metadata);

  default void sendAndForget(Object payload) {
    sendAndForget(payload, null);
  }

  <R> CompletableFuture<R> send(Object payload, Metadata metadata);

  default <R> CompletableFuture<R> send(Object payload) {
    return send(payload, null);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload, Metadata metadata) {
    CompletableFuture<R> future = send(payload);
    return future.get(1, TimeUnit.MINUTES);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload) {
    return sendAndWait(payload, null);
  }

}

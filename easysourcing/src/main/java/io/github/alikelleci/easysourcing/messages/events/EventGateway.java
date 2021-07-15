package io.github.alikelleci.easysourcing.messages.events;

import io.github.alikelleci.easysourcing.messages.Gateway;
import io.github.alikelleci.easysourcing.messages.Metadata;


public interface EventGateway extends Gateway {

  void publish(Object payload, Metadata metadata);

  default void publish(Object payload) {
    publish(payload, null);
  }

}

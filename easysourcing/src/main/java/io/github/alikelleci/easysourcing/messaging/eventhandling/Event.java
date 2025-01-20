package io.github.alikelleci.easysourcing.messaging.eventhandling;

import io.github.alikelleci.easysourcing.messaging.Message;
import io.github.alikelleci.easysourcing.messaging.Metadata;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Event extends Message {

  private Event() {
  }

  @Builder
  private Event(Object payload, Metadata metadata) {
    super(payload, metadata);
  }
}

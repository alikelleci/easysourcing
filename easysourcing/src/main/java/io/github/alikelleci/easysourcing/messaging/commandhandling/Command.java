package io.github.alikelleci.easysourcing.messaging.commandhandling;

import io.github.alikelleci.easysourcing.messaging.Message;
import io.github.alikelleci.easysourcing.messaging.Metadata;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Command extends Message {

  private Command() {
  }

  @Builder
  private Command(Object payload, Metadata metadata) {
    super(payload, metadata);
  }

}

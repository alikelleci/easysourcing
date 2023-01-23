package io.github.alikelleci.easysourcing.messaging.eventsourcing;

import io.github.alikelleci.easysourcing.messaging.Message;
import io.github.alikelleci.easysourcing.messaging.Metadata;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Aggregate extends Message {

  protected Aggregate() {
  }

  @Builder
  protected Aggregate(Object payload, Metadata metadata) {
    super(payload, metadata);
  }

}

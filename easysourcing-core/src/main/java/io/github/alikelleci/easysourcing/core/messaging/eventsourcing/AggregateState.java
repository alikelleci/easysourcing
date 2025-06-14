package io.github.alikelleci.easysourcing.core.messaging.eventsourcing;

import io.github.alikelleci.easysourcing.core.messaging.Message;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class AggregateState extends Message {

  private AggregateState() {
  }

  @Builder
  private AggregateState(Object payload, Metadata metadata) {
    super(payload, metadata);
  }

}

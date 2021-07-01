package io.github.alikelleci.easysourcing.messages.events;

import io.github.alikelleci.easysourcing.messages.Message;
import io.github.alikelleci.easysourcing.messages.Metadata;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class Event<T> implements Message<T> {

  private T payload;
  private Metadata metadata;

  @Override
  public T getPayload() {
    return payload;
  }

  @Override
  public Metadata getMetadata() {
    if (metadata == null) {
      return new Metadata();
    }
    return metadata;
  }
}

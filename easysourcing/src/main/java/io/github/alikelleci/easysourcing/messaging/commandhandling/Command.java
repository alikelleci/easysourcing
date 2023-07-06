package io.github.alikelleci.easysourcing.messaging.commandhandling;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.messaging.Message;
import io.github.alikelleci.easysourcing.messaging.Metadata;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;
import java.util.Optional;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Command extends Message {
  private String aggregateId;

  protected Command() {
    this.aggregateId = null;
  }

  @Builder
  protected Command(Instant timestamp, Object payload, Metadata metadata) {
    super(timestamp, payload, metadata);

    this.aggregateId = Optional.ofNullable(payload)
        .flatMap(p -> FieldUtils.getFieldsListWithAnnotation(p.getClass(), AggregateId.class).stream()
            .filter(field -> field.getType() == String.class)
            .findFirst()
            .map(field -> {
              field.setAccessible(true);
              return (String) ReflectionUtils.getField(field, p);
            }))
        .orElse(null);
  }
}

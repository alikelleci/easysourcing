package io.github.alikelleci.easysourcing.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.Optional;

import static io.github.alikelleci.easysourcing.messaging.Metadata.ID;
import static io.github.alikelleci.easysourcing.messaging.Metadata.TIMESTAMP;

@Getter
@ToString
@EqualsAndHashCode
public class Message {
  private String id;
  private Instant timestamp;
  private String type;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private Object payload;
  private Metadata metadata;

  protected Message() {
  }

  protected Message(Instant timestamp, Object payload, Metadata metadata) {
    this.id = Optional.ofNullable(payload)
        .flatMap(p -> FieldUtils.getFieldsListWithAnnotation(p.getClass(), AggregateId.class).stream()
            .filter(field -> field.getType() == String.class)
            .findFirst()
            .map(field -> {
              field.setAccessible(true);
              return (String) ReflectionUtils.getField(field, p);
            }))
        .map(s -> s + "@" + UlidCreator.getMonotonicUlid().toString())
        .orElse(null);

    this.timestamp = Optional.ofNullable(timestamp)
        .orElse(Instant.now());

    this.type = Optional.ofNullable(payload)
        .map(Object::getClass)
        .map(Class::getSimpleName)
        .orElse(null);

    this.payload = payload;

    this.metadata = Metadata.builder()
        .addAll(metadata)
        .add(ID, this.id)
        .add(TIMESTAMP, this.timestamp.toString())
        .build();
  }

  @Transient
  public TopicInfo getTopicInfo() {
    return Optional.ofNullable(getPayload())
        .map(p -> AnnotationUtils.findAnnotation(p.getClass(), TopicInfo.class))
        .orElse(null);
  }

}
package io.github.alikelleci.easysourcing.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.support.serializer.custom.MetadataDeserializer;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;
import java.util.Optional;
import java.util.UUID;

import static io.github.alikelleci.easysourcing.messaging.Metadata.ID;

@Getter
@ToString
@EqualsAndHashCode
public abstract class Message {
  private String type;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private Object payload;
  @JsonDeserialize(using = MetadataDeserializer.class)
  private Metadata metadata;

  protected Message() {
  }

  protected Message(Object payload, Metadata metadata) {
    this.type = Optional.ofNullable(payload)
        .map(Object::getClass)
        .map(Class::getSimpleName)
        .orElse(null);

    this.payload = payload;

    this.metadata = Metadata.builder()
        .addAll(metadata)
        .add(ID, UUID.randomUUID().toString())
        .build();
  }

  @Transient
  public String getAggregateId() {
    return Optional.ofNullable(getPayload())
        .flatMap(p -> FieldUtils.getFieldsListWithAnnotation(p.getClass(), AggregateId.class).stream()
            .filter(field -> field.getType() == String.class)
            .findFirst()
            .map(field -> {
              field.setAccessible(true);
              return (String) ReflectionUtils.getField(field, p);
            }))
        .orElse(null);
  }

  @Transient
  public TopicInfo getTopicInfo() {
    return Optional.ofNullable(getPayload())
        .map(p -> AnnotationUtils.findAnnotation(p.getClass(), TopicInfo.class))
        .orElse(null);
  }

}

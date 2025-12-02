package io.github.alikelleci.easysourcing.core.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.easysourcing.core.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.core.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.core.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.easysourcing.core.common.exceptions.TopicInfoMissingException;
import io.github.alikelleci.easysourcing.core.support.serialization.json.custom.MetadataDeserializer;
import io.github.alikelleci.easysourcing.core.util.AnnotationUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.commons.lang3.reflect.FieldUtils;
import tools.jackson.databind.annotation.JsonDeserialize;

import java.beans.Transient;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.UUID;

import static io.github.alikelleci.easysourcing.core.messaging.Metadata.ID;

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
    return FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class)
        .stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> getFieldValue(field, payload))
        .orElseThrow(() -> new AggregateIdMissingException("Aggregate identifier missing. Please annotate your field containing the identifier with @AggregateId."));
  }

  @Transient
  public TopicInfo getTopicInfo() {
    return Optional.ofNullable(getPayload())
        .map(p -> AnnotationUtils.findAnnotation(p.getClass(), TopicInfo.class))
        .orElseThrow(() -> new TopicInfoMissingException("Topic information not found. Please annotate your payload class with @TopicInfo."));
  }

  @SneakyThrows
  private static String getFieldValue(Field field, Object target) {
    field.setAccessible(true);
    Object value = field.get(target);
    if (value == null) {
      throw new AggregateIdMissingException("Aggregate identifier cannot be null.");
    }
    return value.toString();
  }
}

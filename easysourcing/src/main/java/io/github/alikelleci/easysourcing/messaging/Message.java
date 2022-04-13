package io.github.alikelleci.easysourcing.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;
import java.util.Optional;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
public class Message {

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private Object payload;
  private Metadata metadata;

  public Metadata getMetadata() {
    if (metadata == null) {
      return Metadata.builder().build();
    }
    return metadata;
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

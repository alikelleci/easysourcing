package io.github.alikelleci.easysourcing.messages;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;


@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public interface Message<T> {

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  T getPayload();

  Metadata getMetadata();

  @Transient
  default String getAggregateId() {
    if (getPayload() == null) {
      return null;
    }

    return FieldUtils.getFieldsListWithAnnotation(getPayload().getClass(), AggregateId.class).stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, getPayload());
        })
        .orElse(null);
  }

  @Transient
  default TopicInfo getTopicInfo() {
    if (getPayload() == null) {
      return null;
    }
    return AnnotationUtils.findAnnotation(getPayload().getClass(), TopicInfo.class);
  }

  @Transient
  default String getMessageType() {
    return getClass().getSimpleName();
  }

  @Transient
  default String getPayloadType() {
    if (getPayload() == null) {
      return null;
    }
    return getPayload().getClass().getSimpleName();
  }

}

package com.github.easysourcing.message;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.easysourcing.message.annotations.AggregateId;
import com.github.easysourcing.message.annotations.TopicInfo;
import org.springframework.core.annotation.AnnotationUtils;

import java.beans.Transient;
import java.lang.reflect.Field;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public interface Message<T> {

  default String getType() {
    if (getPayload() == null) {
      return null;
    }
    return getPayload().getClass().getSimpleName();
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  T getPayload();

  Metadata getMetadata();

  @Transient
  default String getId() {
    if (getPayload() == null) {
      return null;
    }

    for (Field field : getPayload().getClass().getDeclaredFields()) {
      AggregateId annotation = field.getAnnotation(AggregateId.class);
      if (annotation != null) {
        field.setAccessible(true);
        try {
          Object value = field.get(getPayload());
          if (value instanceof String) {
            return (String) value;
          }
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      }
    }
    return null;
  }

  @Transient
  default TopicInfo getTopicInfo() {
    if (getPayload() == null) {
      return null;
    }
    return AnnotationUtils.findAnnotation(getPayload().getClass(), TopicInfo.class);
  }

}

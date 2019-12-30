package com.github.easysourcing.message;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.easysourcing.message.annotations.AggregateId;
import com.github.easysourcing.message.annotations.TopicInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.core.annotation.AnnotationUtils;

import java.beans.Transient;
import java.lang.reflect.Field;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class Message {

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private Object payload;
  private Metadata metadata;

  @Transient
  public String getId() {
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
  public TopicInfo getTopicInfo() {
    if (getPayload() == null) {
      return null;
    }
    return AnnotationUtils.findAnnotation(getPayload().getClass(), TopicInfo.class);
  }

}

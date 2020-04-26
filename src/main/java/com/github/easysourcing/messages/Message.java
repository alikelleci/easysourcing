package com.github.easysourcing.messages;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.easysourcing.messages.annotations.AggregateId;
import com.github.easysourcing.messages.annotations.TopicInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;

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
  public TopicInfo getTopicInfo() {
    if (getPayload() == null) {
      return null;
    }
    return AnnotationUtils.findAnnotation(getPayload().getClass(), TopicInfo.class);
  }

}

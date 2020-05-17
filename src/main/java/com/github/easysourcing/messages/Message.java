package com.github.easysourcing.messages;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.easysourcing.messages.annotations.AggregateId;
import com.github.easysourcing.messages.annotations.TopicInfo;
import com.github.easysourcing.messages.commands.Command;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;
import java.util.HashMap;
import java.util.Map;

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
  public String getAggregateId() {
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


  public Metadata getMetadata() {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    if (!(this instanceof Command)) {
      Map<String, String> modified = new HashMap<>(metadata.getEntries());
      modified.keySet().removeIf(key ->
          StringUtils.equalsAny(key, "$result", "$events", "$failure"));

      return metadata.toBuilder()
          .clearEntries()
          .entries(modified)
          .build();
    }
    return metadata;
  }
}

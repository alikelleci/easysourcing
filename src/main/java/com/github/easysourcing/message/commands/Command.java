package com.github.easysourcing.message.commands;

import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.Metadata;
import com.github.easysourcing.message.annotations.AggregateId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Builder;
import lombok.Value;

import java.beans.Transient;
import java.lang.reflect.Field;

@Value
@Builder(toBuilder = true)
public class Command<T> implements Message {

  private String type;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private T payload;
  private Metadata metadata;

  @Transient
  public String getAggregateId() {
    for (Field field : payload.getClass().getDeclaredFields()) {
      AggregateId annotation = field.getAnnotation(AggregateId.class);
      if (annotation != null) {
        field.setAccessible(true);
        try {
          Object value = field.get(payload);
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

}

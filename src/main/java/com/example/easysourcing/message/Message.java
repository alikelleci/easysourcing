package com.example.easysourcing.message;

import com.example.easysourcing.message.annotations.AggregateId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.beans.Transient;
import java.lang.reflect.Field;

@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class Message<T> {

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private T payload;
  private MessageType type;


  @Builder(toBuilder = true)
  public Message(T payload, MessageType type) {
    this.payload = payload;
    this.type = type;
  }

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

  public T getPayload() {
    return payload;
  }

  public String getName() {
    return payload.getClass().getSimpleName();
  }

  public MessageType getType() {
    return type;
  }
}

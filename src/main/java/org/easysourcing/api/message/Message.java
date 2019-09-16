package org.easysourcing.api.message;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Builder;
import lombok.ToString;
import org.easysourcing.api.message.annotations.AggregateId;

import java.beans.Transient;
import java.lang.reflect.Field;

@ToString
@Builder(toBuilder = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class Message<T> {

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private T payload;
  private MessageType type;

  public Message() {
  }

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

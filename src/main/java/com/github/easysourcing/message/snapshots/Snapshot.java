package com.github.easysourcing.message.snapshots;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.easysourcing.message.annotations.AggregateId;
import lombok.Builder;
import lombok.ToString;

import java.beans.Transient;
import java.lang.reflect.Field;

@ToString
@Builder(toBuilder = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class Snapshot<T> {

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private T payload;


  public Snapshot() {
  }

  public Snapshot(T payload) {
    this.payload = payload;
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

  public String getType() {
    return payload.getClass().getSimpleName();
  }

}

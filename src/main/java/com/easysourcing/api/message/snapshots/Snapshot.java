package com.easysourcing.api.message.snapshots;

import com.easysourcing.api.message.annotations.AggregateId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.beans.Transient;
import java.lang.reflect.Field;

@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class Snapshot<T> {

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private T data;


  @Builder(toBuilder = true)
  public Snapshot(T data) {
    this.data = data;
  }

  @Transient
  public String getAggregateId() {
    for (Field field : data.getClass().getDeclaredFields()) {
      AggregateId annotation = field.getAnnotation(AggregateId.class);
      if (annotation != null) {
        field.setAccessible(true);
        try {
          Object value = field.get(data);
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

  public T getData() {
    return data;
  }

  public String getType() {
    return data.getClass().getSimpleName();
  }

}

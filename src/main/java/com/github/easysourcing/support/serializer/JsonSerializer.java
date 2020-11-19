package com.github.easysourcing.support.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.easysourcing.support.JacksonUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

  private final ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();

  public JsonSerializer() {
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, T data) {
    if (data == null)
      return null;

    try {
      return objectMapper.writeValueAsBytes(data);
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON", e);
    }
  }

  @Override
  public void close() {
  }

}

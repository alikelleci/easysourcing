package com.github.easysourcing.support.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.easysourcing.support.JacksonUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

  private final ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();
  private Class<T> type;

  public JsonDeserializer() {
  }

  public JsonDeserializer(Class<T> type) {
    this.type = type;
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    
    try {
      return objectMapper.readValue(bytes, type);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", e);
    }
  }

  @Override
  public void close() {
  }

}

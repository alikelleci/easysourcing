package io.github.alikelleci.easysourcing.core.support.serialization.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.core.support.serialization.json.util.JacksonUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

  private final ObjectMapper objectMapper;

  public JsonSerializer() {
    this(JacksonUtils.enhancedObjectMapper());
  }

  public JsonSerializer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, T object) {
    if (object == null) {
      return new byte[0];
    }

    try {
      return objectMapper.writeValueAsBytes(object);

    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON", ExceptionUtils.getRootCause(e));
    }
  }

  @Override
  public void close() {
  }

}

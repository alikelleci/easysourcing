package io.github.alikelleci.easysourcing.core.support.serialization.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.core.support.serialization.json.util.JacksonUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

  private final Class<T> targetType;
  private final ObjectMapper objectMapper;


  public JsonDeserializer() {
    this(null);
  }

  public JsonDeserializer(Class<T> targetType) {
    this(targetType, JacksonUtils.enhancedObjectMapper());

  }

  public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper) {
    this.targetType = targetType;
    this.objectMapper = objectMapper;
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
      return objectMapper.readValue(bytes, targetType);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", ExceptionUtils.getRootCause(e));
    }
  }

  @Override
  public void close() {
  }

}

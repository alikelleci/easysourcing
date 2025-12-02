package io.github.alikelleci.easysourcing.core.support.serialization.json;

import io.github.alikelleci.easysourcing.core.support.serialization.json.util.JacksonUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

  private final Class<T> targetType;
  private final JsonMapper jsonMapper;


  public JsonDeserializer() {
    this(null);
  }

  public JsonDeserializer(Class<T> targetType) {
    this(targetType, JacksonUtils.enhancedJsonMapper());
  }

  public JsonDeserializer(Class<T> targetType, JsonMapper jsonMapper) {
    this.targetType = targetType;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    if (bytes == null) return null;
    try {
      return jsonMapper.readValue(bytes, targetType);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", e);
    }
  }

  @Override
  public void close() {
  }

}

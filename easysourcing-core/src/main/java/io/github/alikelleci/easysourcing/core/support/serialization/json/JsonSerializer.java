package io.github.alikelleci.easysourcing.core.support.serialization.json;

import io.github.alikelleci.easysourcing.core.support.serialization.json.util.JacksonUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

  private final JsonMapper jsonMapper;

  public JsonSerializer() {
    this(JacksonUtils.enhancedJsonMapper());
  }

  public JsonSerializer(JsonMapper jsonMapper) {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, T object) {
    if (object == null) return null;
    try {
      return jsonMapper.writeValueAsBytes(object);
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON", e);
    }
  }

  @Override
  public void close() {
  }

}

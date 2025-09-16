package io.github.alikelleci.easysourcing.core.support.serialization.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.core.support.serialization.json.util.JacksonUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerde<T> implements Serde<T> {
  private final JsonSerializer<T> serializer;
  private final JsonDeserializer<T> deserializer;

  public JsonSerde(Class<T> targetType) {
    this(targetType, JacksonUtils.enhancedObjectMapper());
  }

  public JsonSerde(Class<T> targetType, ObjectMapper objectMapper) {
    this.serializer = new JsonSerializer<>(objectMapper);
    this.deserializer = new JsonDeserializer<>(targetType, objectMapper);
  }

  @Override
  public Serializer<T> serializer() {
    return this.serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return this.deserializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.serializer.configure(configs, isKey);
    this.deserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    this.serializer.close();
    this.deserializer.close();
  }
}

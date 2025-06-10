package io.github.alikelleci.easysourcing.core.support.serialization.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.core.support.serialization.json.util.JacksonUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerde<T> implements Serde<T> {
  private final JsonSerializer<T> jsonSerializer;
  private final JsonDeserializer<T> jsonDeserializer;

  public JsonSerde(Class<T> targetType) {
    this(targetType, JacksonUtils.enhancedObjectMapper());
  }

  public JsonSerde(Class<T> targetType, ObjectMapper objectMapper) {
    this.jsonSerializer = new JsonSerializer<>(targetType, objectMapper);
    this.jsonDeserializer = new JsonDeserializer<>(targetType, objectMapper);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.jsonSerializer.configure(configs, isKey);
    this.jsonDeserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    this.jsonSerializer.close();
    this.jsonDeserializer.close();
  }

  @Override
  public Serializer<T> serializer() {
    return this.jsonSerializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return this.jsonDeserializer;
  }
}

package io.github.alikelleci.easysourcing.support.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.easysourcing.util.JacksonUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
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
      throw new SerializationException("Error deserializing JSON", ExceptionUtils.getRootCause(e));
    }
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try {
      ObjectNode root = objectMapper.readValue(bytes, ObjectNode.class);
      String className = root.get("@class").textValue();

      // Added for backwards compatibility
      if (className.startsWith("com.github.easysourcing.")) {
        // Root class name correction
        className = className.replace("com.github.easysourcing.", "io.github.alikelleci.easysourcing.");
        root.put("@class", className);

        // Aggregate --> Snapshot
        if (className.endsWith(".aggregates.Aggregate")) {
          className = className.replace(".aggregates.Aggregate", ".snapshots.Snapshot");
          root.put("@class", className);
        }

        // Metadata correction
        ObjectNode metadata = (ObjectNode) root.get("metadata");
        ObjectNode entries = (ObjectNode) metadata.get("entries");
        metadata.remove("entries");
        metadata.set("values", entries);

        // Write to bytes
        bytes = objectMapper.writeValueAsBytes(root);
      }
    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", ExceptionUtils.getRootCause(e));
    }

    return deserialize(topic, bytes);
  }

  @Override
  public void close() {
  }

}

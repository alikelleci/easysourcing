package io.github.alikelleci.easysourcing.support.serializer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class CustomSerdes {

  public static final class JsonSerde<T> extends Serdes.WrapperSerde<T> {
    public JsonSerde() {
      super(new JsonSerializer<>(), new JsonDeserializer(JsonNode.class));
    }

    public JsonSerde(Class<T> tClass) {
      super(new JsonSerializer<>(), new JsonDeserializer<>(tClass));
    }
  }

  public static <T> Serde<T> Json() {
    return new CustomSerdes.JsonSerde<>();
  }

  public static <T> Serde<T> Json(Class<T> tClass) {
    return new CustomSerdes.JsonSerde<>(tClass);
  }

}

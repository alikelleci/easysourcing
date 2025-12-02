package io.github.alikelleci.easysourcing.core.support.serialization.json.custom;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.Instant;

public class InstantDeserializer extends StdDeserializer<Instant> {

  public InstantDeserializer() {
    this(Instant.class);
  }

  protected InstantDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public Instant deserialize(JsonParser jp, DeserializationContext deserializationContext) throws IOException {
    ObjectMapper mapper = (ObjectMapper) jp.getCodec();
    JsonNode node = mapper.readTree(jp);

    if (node == null) {
      return null;
    }

    if (node.isNumber()) {
      return Instant.ofEpochMilli(toMillis(node.asLong()));
    }

    if (node.isTextual()) {
      return Instant.parse(node.asText());
    }

    return null;
  }


  private long toMillis(long timestamp) {

    // nanoseconds
    if (timestamp >= 1E16 || timestamp <= -1E16) {
      return timestamp / 1_000_000;
    }

    // microseconds
    if (timestamp >= 1E14 || timestamp <= -1E14) {
      return timestamp / 1_000;
    }

    // milliseconds
    if (timestamp >= 1E11 || timestamp <= -3E10) {
      return timestamp;
    }

    // seconds
    return timestamp * 1_000;
  }
}

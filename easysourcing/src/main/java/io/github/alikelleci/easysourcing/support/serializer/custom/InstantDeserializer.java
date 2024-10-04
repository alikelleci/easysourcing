package io.github.alikelleci.easysourcing.support.serializer.custom;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.time.Instant;

public class InstantDeserializer extends JsonDeserializer<Instant> {

  public InstantDeserializer() {
  }

  @Override
  public Instant deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);

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

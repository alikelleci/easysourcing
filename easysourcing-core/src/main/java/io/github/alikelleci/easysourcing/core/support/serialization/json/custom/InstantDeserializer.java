package io.github.alikelleci.easysourcing.core.support.serialization.json.custom;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;

import java.time.Instant;

public class InstantDeserializer extends StdDeserializer<Instant> {

  public InstantDeserializer() {
    this(null);
  }

  protected InstantDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
    JsonNode jsonNode = ctxt.readTree(p);

    if (jsonNode == null) {
      return null;
    }

    if (jsonNode.isNumber()) {
      return Instant.ofEpochMilli(toMillis(jsonNode.asLong()));
    }

    if (jsonNode.isString()) {
      return Instant.parse(jsonNode.asString());
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

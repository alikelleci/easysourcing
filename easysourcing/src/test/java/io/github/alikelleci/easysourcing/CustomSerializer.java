package io.github.alikelleci.easysourcing;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.time.Instant;

public class CustomSerializer extends StdSerializer<Instant> {

  public CustomSerializer() {
    this(null);
  }

  protected CustomSerializer(Class<Instant> t) {
    super(t);
  }

  @Override
  public void serialize(Instant instant, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {

  }
}

package io.github.alikelleci.easysourcing.support.serializer.custom;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.collections4.MultiValuedMap;

import java.io.IOException;

public class MultiValuedMapSerializer extends StdSerializer<MultiValuedMap<String, ?>> {

  public MultiValuedMapSerializer() {
    this(null);
  }

  protected MultiValuedMapSerializer(Class<MultiValuedMap<String, ?>> t) {
    super(t);
  }

  @Override
  public void serialize(MultiValuedMap<String, ?> value, JsonGenerator generator, SerializerProvider serializerProvider) throws IOException {
    if (value == null) {
      generator.writeNull();
      return;
    }
    generator.writeObject(value.asMap());
  }
}
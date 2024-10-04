package io.github.alikelleci.easysourcing.support.serializer.custom;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.collections4.MultiValuedMap;

import java.io.IOException;

public class MultiValuedMapSerializer extends JsonSerializer<MultiValuedMap<String, ?>> {

  public MultiValuedMapSerializer() {
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
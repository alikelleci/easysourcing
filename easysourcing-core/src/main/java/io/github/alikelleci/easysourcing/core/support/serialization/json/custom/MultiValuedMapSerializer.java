package io.github.alikelleci.easysourcing.core.support.serialization.json.custom;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.collections4.MultiValuedMap;

import java.io.IOException;

public class MultiValuedMapSerializer extends StdSerializer<MultiValuedMap> {

  public MultiValuedMapSerializer() {
    this(MultiValuedMap.class);
  }

  protected MultiValuedMapSerializer(Class<MultiValuedMap> t) {
    super(t);
  }

  @Override
  public void serialize(MultiValuedMap value, JsonGenerator generator, SerializerProvider serializerProvider) throws IOException {
    if (value == null) {
      generator.writeNull();
      return;
    }
    generator.writeObject(value.asMap());
  }
}
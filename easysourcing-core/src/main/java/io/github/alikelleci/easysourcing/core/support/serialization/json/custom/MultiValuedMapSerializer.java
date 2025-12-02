package io.github.alikelleci.easysourcing.core.support.serialization.json.custom;

import org.apache.commons.collections4.MultiValuedMap;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ser.std.StdSerializer;

public class MultiValuedMapSerializer extends StdSerializer<MultiValuedMap> {

  public MultiValuedMapSerializer() {
    this(MultiValuedMap.class);
  }

  protected MultiValuedMapSerializer(Class<MultiValuedMap> t) {
    super(t);
  }

  @Override
  public void serialize(MultiValuedMap value, JsonGenerator gen, SerializationContext ctxt) throws JacksonException {
    if (value == null) {
      gen.writeNull();
      return;
    }
    gen.writePOJO(value.asMap());
  }
}
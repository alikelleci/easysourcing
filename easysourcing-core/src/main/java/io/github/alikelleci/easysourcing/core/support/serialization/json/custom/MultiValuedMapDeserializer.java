package io.github.alikelleci.easysourcing.core.support.serialization.json.custom;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.BeanProperty;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.type.TypeBindings;
import tools.jackson.databind.type.TypeFactory;

import java.util.Collection;

public class MultiValuedMapDeserializer extends StdDeserializer<MultiValuedMap<String, ?>> {

  private Class<?> valueClass;

  public MultiValuedMapDeserializer() {
    this(MultiValuedMap.class);
  }

  protected MultiValuedMapDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public MultiValuedMap<String, ?> deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
    JsonNode jsonNode = p.objectReadContext().readTree(p);

    MultiValuedMap<String, Object> map = new ArrayListValuedHashMap<>();

    if (jsonNode == null) {
      return map;
    }

    for (String propertyName : jsonNode.propertyNames()) {
      JsonNode values = jsonNode.get(propertyName);

      if (values.isArray()) {
        JavaType javaType = TypeFactory.createDefaultInstance().constructCollectionType(Collection.class, valueClass);
        Collection<?> o = ctxt.readTreeAsValue(values, javaType);
        map.putAll(propertyName, o);
      }
    }

    return map;
  }


  @Override
  public ValueDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
    TypeBindings bindings = property.getType().getBindings();
    this.valueClass = bindings.getBoundType(1).getRawClass();

    return this;
  }
}

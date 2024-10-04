package io.github.alikelleci.easysourcing.support.serializer.custom;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.type.TypeBindings;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class MultiValuedMapDeserializer extends JsonDeserializer<MultiValuedMap<String, ?>> implements ContextualDeserializer {

  private Class<?> valueClass;

  public MultiValuedMapDeserializer() {
  }

  public MultiValuedMapDeserializer(Class<?> valueClass) {
    this.valueClass = valueClass;
  }

  @Override
  public MultiValuedMap<String, ?> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    MultiValuedMap<String, Object> map = new ArrayListValuedHashMap<>();

    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    if (node == null) {
      return map;
    }

    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String key = field.getKey();
      JsonNode values = field.getValue();

      if (values.isArray()) {
        JavaType javaType = TypeFactory.defaultInstance().constructCollectionType(Collection.class, valueClass);
        Collection<?> o = deserializationContext.readTreeAsValue(values, javaType);
        map.putAll(key, o);
      }
    }

    return map;
  }


  @Override
  public JsonDeserializer<?> createContextual(DeserializationContext context, BeanProperty property) {
    if (property == null) {
      return this;
    }

    TypeBindings bindings = property.getType().getBindings();
    Class<?> valueType = bindings.getBoundType(1).getRawClass();
    return new MultiValuedMapDeserializer(valueType);
  }
}

package io.github.alikelleci.easysourcing.support.serializer.custom;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.github.alikelleci.easysourcing.messaging.Metadata;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class MetadataDeserializer extends StdDeserializer<Metadata> {

  public MetadataDeserializer() {
    this(null);
  }

  protected MetadataDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public Metadata deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);

    JsonNode entries = node.get("entries");
    return toMetadata(Objects.requireNonNullElse(entries, node));
  }

  private static Metadata toMetadata(JsonNode node) {
    Metadata metadata = Metadata.builder().build();

    Iterator<Map.Entry<String, JsonNode>> it = node.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();

      String key = entry.getKey();
      String value = Optional.ofNullable(entry.getValue())
          .filter(JsonNode::isTextual)
          .map(JsonNode::textValue)
          .orElse(null);

      if (StringUtils.isNoneBlank(key, value)) {
        metadata.add(key, value);
      }
    }

    return metadata;
  }

}

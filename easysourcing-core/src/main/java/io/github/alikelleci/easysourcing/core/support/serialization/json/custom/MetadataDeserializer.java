package io.github.alikelleci.easysourcing.core.support.serialization.json.custom;


import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import org.apache.commons.lang3.StringUtils;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.TreeNode;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;

import java.util.Objects;
import java.util.Optional;

public class MetadataDeserializer extends StdDeserializer<Metadata> {

  public MetadataDeserializer() {
    this(Metadata.class);
  }

  @Override
  public Metadata deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
    TreeNode treeNode = p.objectReadContext().readTree(p);
    JsonNode jsonNode = ctxt.readTree(p);

    JsonNode entries = jsonNode.get("entries"); // added for backwards compatibility
    return toMetadata(Objects.requireNonNullElse(entries, jsonNode));  }

  protected MetadataDeserializer(Class<?> vc) {
    super(vc);
  }


  private static Metadata toMetadata(JsonNode node) {
    Metadata metadata = Metadata.builder().build();

    for (String propertyName : node.propertyNames()) {
      String value = Optional.ofNullable(node.get(propertyName))
          .filter(JsonNode::isString)
          .map(JsonNode::stringValue)
          .orElse(null);

      if (StringUtils.isNoneBlank(propertyName, value)) {
        metadata.add(propertyName, value);
      }
    }

    return metadata;
  }

}

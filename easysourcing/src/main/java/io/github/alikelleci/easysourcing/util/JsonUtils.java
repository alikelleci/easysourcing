package io.github.alikelleci.easysourcing.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

@Slf4j
@UtilityClass
public class JsonUtils {

  private ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();

  public Object toJavaType(JsonNode jsonNode) {
    if (jsonNode == null) {
      return null;
    }

    String className = Optional.ofNullable(jsonNode)
        .map(payload -> payload.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    if (StringUtils.isBlank(className)) {
      return null;
    }

    try {
      Class<?> type = Class.forName(className);
      return objectMapper.convertValue(jsonNode, type);

    } catch (Exception e) {
      log.error("Failed converting to JavaType", e);
      return null;
    }
  }

  public JsonNode toJsonNode(Object object) {
    if (object == null) {
      return null;
    }

    try {
      ObjectNode root = objectMapper.createObjectNode();
      root.put("@class", object.getClass().getName());

      ObjectNode node = objectMapper.convertValue(object, ObjectNode.class);
      root.setAll(node);

      return root;

    } catch (Exception e) {
      log.error("Failed converting to JsonNode", e);
      return null;
    }
  }

}

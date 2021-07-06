package io.github.alikelleci.easysourcing.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

@UtilityClass
public class JsonUtils {

  private ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();

  public Object toJavaType(JsonNode jsonNode) {
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

    } catch (ClassNotFoundException e) {
      return null;
    }

  }

}

package io.github.alikelleci.easysourcing.messages.upcasters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.easysourcing.messages.Handler;
import io.github.alikelleci.easysourcing.messages.upcasters.annotations.Upcast;
import io.github.alikelleci.easysourcing.messages.upcasters.exceptions.UpcastException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

@Slf4j
public class Upcaster implements Handler<JsonNode> {

  private final Object target;
  private final Method method;

  public Upcaster(Object target, Method method) {
    this.target = target;
    this.method = method;
  }

  @Override
  public JsonNode invoke(Object... args) {
    JsonNode jsonNode = (JsonNode) args[0];

    try {
      return doInvoke(jsonNode);
    } catch (Exception e) {
      throw new UpcastException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private JsonNode doInvoke(JsonNode jsonNode) throws InvocationTargetException, IllegalAccessException {
    int sourceRevision = Optional.ofNullable(jsonNode.get("metadata"))
        .map(metadata -> metadata.get("entries"))
        .map(entries -> entries.get("$revision"))
        .map(JsonNode::intValue)
        .orElse(0);

    int targetRevision = method.getAnnotation(Upcast.class).revision();

    if (sourceRevision != targetRevision) {
      return jsonNode;
    }

    log.debug("Upcasting revision: {}", sourceRevision);

    Object result = method.invoke(target, jsonNode.get("payload"));
    if (result == null) {
      ((ObjectNode) jsonNode).remove("payload");
    } else {
      ((ObjectNode) jsonNode).set("payload", (JsonNode) result);
    }

    ((ObjectNode) jsonNode.get("metadata").get("entries")).put("$revision", sourceRevision + 1);

    return jsonNode;
  }

  @Override
  public Object getTarget() {
    return target;
  }

  @Override
  public Method getMethod() {
    return method;
  }

  @Override
  public Class<?> getType() {
    return method.getParameters()[0].getType();
  }

}

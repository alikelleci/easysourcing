package io.github.alikelleci.easysourcing.messages.upcasters;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Handler;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.upcasters.annotations.Upcast;
import io.github.alikelleci.easysourcing.messages.upcasters.exceptions.UpcastException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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
    Metadata metadata = (Metadata) args[1];

    try {
      return doInvoke(jsonNode, metadata);
    } catch (Exception e) {
      throw new UpcastException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private JsonNode doInvoke(JsonNode jsonNode, Metadata metadata) throws InvocationTargetException, IllegalAccessException {
    Upcast annotation = method.getAnnotation(Upcast.class);
    int revision = Integer.parseInt(metadata.get("$revision"));

    if (revision != annotation.revision()) {
      return jsonNode;
    }

    log.debug("Upcasting type {} (revision: {})", annotation.type(), revision);

    Object result = method.invoke(target, jsonNode);
    if (JsonNode.class.isAssignableFrom(result.getClass())) {
      return (JsonNode) result;
    }
    return null;
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

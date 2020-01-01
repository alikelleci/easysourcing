package com.github.easysourcing.message.aggregates;

import com.github.easysourcing.message.Handler;
import com.github.easysourcing.message.aggregates.exceptions.AggregateInvocationException;
import com.github.easysourcing.message.events.Event;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class AggregateHandler implements Handler<Aggregate> {

  private Object target;
  private Method method;

  public AggregateHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
  }

  @Override
  public Aggregate invoke(Object... args) {
    Aggregate aggregate = (Aggregate) args[0];
    Event event = (Event) args[1];

    Object result;
    try {
      if (method.getParameterCount() == 2) {
        result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload());
      } else {
        result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload(), event.getMetadata());
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new AggregateInvocationException(e.getCause().getMessage(), e.getCause());
    }

    if (result == null) {
      return null;
    }

    return Aggregate.builder()
        .payload(result)
        .metadata(event.getMetadata())
        .build();
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
    return method.getParameters()[1].getType();
  }

}

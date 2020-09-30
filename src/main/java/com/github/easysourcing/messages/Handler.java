package com.github.easysourcing.messages;

import com.github.easysourcing.messages.annotations.Order;

import java.lang.reflect.Method;

public interface Handler<R> {

  R invoke(Object... args);

  Object getTarget();

  Method getMethod();

  Class<?> getType();

  default int getOrder() {
    Order annotation = getMethod().getAnnotation(Order.class);
    if (annotation != null) {
      return annotation.value();
    }
    return 0;
  }
}

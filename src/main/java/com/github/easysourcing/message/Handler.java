package com.github.easysourcing.message;

import java.lang.reflect.Method;

public interface Handler<R> {

  R invoke(Object... args);

  Object getTarget();

  Method getMethod();

  Class<?> getType();

}

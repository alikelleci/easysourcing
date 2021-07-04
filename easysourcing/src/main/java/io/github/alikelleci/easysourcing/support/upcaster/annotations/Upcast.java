package io.github.alikelleci.easysourcing.support.upcaster.annotations;

import io.github.alikelleci.easysourcing.common.annotations.HandleMessage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@HandleMessage
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Upcast {
  Class<?> type();

  int revision();
}

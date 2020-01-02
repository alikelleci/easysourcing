package com.github.easysourcing.message.aggregates.annotations;

import com.github.easysourcing.message.annotations.Handler;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Handler
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Aggregator {
  @AliasFor(
      annotation = Handler.class
  )
  String value() default "";
}

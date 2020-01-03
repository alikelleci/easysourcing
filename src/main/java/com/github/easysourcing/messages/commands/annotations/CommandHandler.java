package com.github.easysourcing.messages.commands.annotations;

import com.github.easysourcing.messages.annotations.Handler;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Handler
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface CommandHandler {
  @AliasFor(
      annotation = Handler.class
  )
  String value() default "";
}

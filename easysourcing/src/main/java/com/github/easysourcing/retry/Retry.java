package com.github.easysourcing.retry;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Retry {

  int attempts() default 3;

  int delay() default 1000;

  Backoff backoff() default Backoff.FIXED;

  Class<? extends Throwable>[] exceptions() default {};

}

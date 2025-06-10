package io.github.alikelleci.easysourcing.core.messaging.resulthandling.annotations;

import io.github.alikelleci.easysourcing.core.common.annotations.HandleMessage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@HandleMessage
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface HandleResult {

}

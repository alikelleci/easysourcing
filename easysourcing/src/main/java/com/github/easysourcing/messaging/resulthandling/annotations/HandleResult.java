package com.github.easysourcing.messaging.resulthandling.annotations;

import com.github.easysourcing.common.annotations.HandleMessage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@HandleMessage
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface HandleResult {

}

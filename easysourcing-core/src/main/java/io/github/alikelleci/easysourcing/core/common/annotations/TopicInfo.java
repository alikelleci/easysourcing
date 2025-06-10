package io.github.alikelleci.easysourcing.core.common.annotations;

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@JacksonAnnotationsInside
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public @interface TopicInfo {
  String value();
}

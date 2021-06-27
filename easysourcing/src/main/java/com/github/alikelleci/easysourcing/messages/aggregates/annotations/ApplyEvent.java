package com.github.alikelleci.easysourcing.messages.aggregates.annotations;

import com.github.alikelleci.easysourcing.messages.annotations.HandleMessage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@HandleMessage
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ApplyEvent {

}

package io.github.alikelleci.easysourcing.core.util;

import io.github.alikelleci.easysourcing.core.EasySourcing;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.easysourcing.core.messaging.eventhandling.EventHandler;
import io.github.alikelleci.easysourcing.core.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.easysourcing.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.easysourcing.core.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.easysourcing.core.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.easysourcing.core.messaging.resulthandling.annotations.HandleResult;
import lombok.experimental.UtilityClass;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class HandlerUtils {

  public void registerHandler(EasySourcing easySourcing, Object handler) {
    List<Method> commandHandlerMethods = findMethodsWithAnnotation(handler.getClass(), HandleCommand.class);
    List<Method> eventSourcingMethods = findMethodsWithAnnotation(handler.getClass(), ApplyEvent.class);
    List<Method> resultHandlerMethods = findMethodsWithAnnotation(handler.getClass(), HandleResult.class);
    List<Method> eventHandlerMethods = findMethodsWithAnnotation(handler.getClass(), HandleEvent.class);

    commandHandlerMethods
        .forEach(method -> addCommandHandler(easySourcing, handler, method));

    eventSourcingMethods
        .forEach(method -> addEventSourcingHandler(easySourcing, handler, method));

    resultHandlerMethods
        .forEach(method -> addResultHandler(easySourcing, handler, method));

    eventHandlerMethods
        .forEach(method -> addEventHandler(easySourcing, handler, method));
  }

  private <A extends Annotation> List<Method> findMethodsWithAnnotation(Class<?> c, Class<A> annotation) {
    List<Method> methods = new ArrayList<>();
    for (Method method : c.getDeclaredMethods()) {
      if (AnnotationUtils.findAnnotation(method, annotation) != null) {
        methods.add(method);
      }
    }
    return methods;
  }

  private void addCommandHandler(EasySourcing easySourcing, Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[1].getType();
      easySourcing.getCommandHandlers().put(type, new CommandHandler(listener, method));
    }
  }

  private void addEventSourcingHandler(EasySourcing easySourcing, Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[1].getType();
      easySourcing.getEventSourcingHandlers().put(type, new EventSourcingHandler(listener, method));
    }
  }

  private void addResultHandler(EasySourcing easySourcing, Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      easySourcing.getResultHandlers().put(type, new ResultHandler(listener, method));
    }
  }

  private void addEventHandler(EasySourcing easySourcing, Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      easySourcing.getEventHandlers().put(type, new EventHandler(listener, method));
    }
  }
}

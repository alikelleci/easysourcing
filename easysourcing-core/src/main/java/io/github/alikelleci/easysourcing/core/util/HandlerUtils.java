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

import java.lang.reflect.Method;

@UtilityClass
public class HandlerUtils {

  public void registerHandler(EasySourcing easySourcing, Object handler) {
    AnnotationUtils.findAnnotatedMethods(handler.getClass(), HandleCommand.class)
        .forEach(method -> addCommandHandler(easySourcing, handler, method));

    AnnotationUtils.findAnnotatedMethods(handler.getClass(), ApplyEvent.class)
        .forEach(method -> addEventSourcingHandler(easySourcing, handler, method));

    AnnotationUtils.findAnnotatedMethods(handler.getClass(), HandleResult.class)
        .forEach(method -> addResultHandler(easySourcing, handler, method));

    AnnotationUtils.findAnnotatedMethods(handler.getClass(), HandleEvent.class)
        .forEach(method -> addEventHandler(easySourcing, handler, method));
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

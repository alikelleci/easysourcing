package com.github.easysourcing.message.events;

import com.github.easysourcing.message.Handler;
import com.github.easysourcing.message.Metadata;
import com.github.easysourcing.message.commands.Command;
import com.github.easysourcing.message.events.exceptions.EventProcessingException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EventHandler implements Handler<List<Command>> {

  private Object target;
  private Method method;

  public EventHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
  }

  @Override
  public List<Command> invoke(Object... args) {
    Event event = (Event) args[0];

    Object result;
    try {
      if (method.getParameterCount() == 1) {
        result = method.invoke(target, event.getPayload());
      } else {
        result = method.invoke(target, event.getPayload(), event.getMetadata());
      }
      return createCommands(result, event.getMetadata());

    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new EventProcessingException(e.getCause().getMessage(), e.getCause());
    }
  }

  @Override
  public Object getTarget() {
    return target;
  }

  @Override
  public Method getMethod() {
    return method;
  }

  @Override
  public Class<?> getType() {
    return method.getParameters()[0].getType();
  }

  private List<Command> createCommands(Object result, Metadata metadata) {
    if (result == null) {
      return new ArrayList<>();
    }

    List<Object> list = new ArrayList<>();
    if (List.class.isAssignableFrom(result.getClass())) {
      list.addAll((List<?>) result);
    } else {
      list.add(result);
    }

    List<Command> commands = list.stream()
        .map(payload -> Command.builder()
            .payload(payload)
            .metadata(metadata)
            .build())
        .collect(Collectors.toList());

    commands.forEach(command -> {
      if (command.getId() == null) {
        throw new EventProcessingException("You are trying to dispatch a command without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
      }
    });

    return commands;
  }

}

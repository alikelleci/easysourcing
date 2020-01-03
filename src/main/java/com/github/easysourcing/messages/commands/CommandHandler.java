package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.commands.exceptions.CommandExecutionException;
import com.github.easysourcing.messages.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class CommandHandler implements Handler<List<Event>> {

  private Object target;
  private Method method;

  public CommandHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
  }

  @Override
  public List<Event> invoke(Object... args) {
    Aggregate aggregate = (Aggregate) args[0];
    Command command = (Command) args[1];

    Object result;
    try {
      if (method.getParameterCount() == 2) {
        result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, command.getPayload());
      } else {
        result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, command.getPayload(), command.getMetadata());
      }
      return createEvents(command, result);

    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new CommandExecutionException(e.getCause().getMessage(), e.getCause());
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
    return method.getParameters()[1].getType();
  }

  private List<Event> createEvents(Command command, Object result) {
    if (result == null) {
      return new ArrayList<>();
    }

    List<Object> list = new ArrayList<>();
    if (List.class.isAssignableFrom(result.getClass())) {
      list.addAll((List<?>) result);
    } else {
      list.add(result);
    }

    List<Event> events = list.stream()
        .map(payload -> Event.builder()
            .payload(payload)
            .metadata(command.getMetadata())
            .build())
        .collect(Collectors.toList());

    events.forEach(event -> {
      if (event.getId() == null) {
        throw new CommandExecutionException("You are trying to dispatch an event without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
      }
      if (!StringUtils.equals(event.getId(), command.getId())) {
        throw new CommandExecutionException("A command-handler can only produce events about the aggregate a given command was targeted to.");
      }
    });

    return events;
  }

}

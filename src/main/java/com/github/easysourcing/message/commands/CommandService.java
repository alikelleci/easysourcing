package com.github.easysourcing.message.commands;

import com.github.easysourcing.message.aggregates.Aggregate;
import com.github.easysourcing.message.commands.exceptions.CommandExecutionException;
import com.github.easysourcing.message.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;


@Slf4j
@Service
public class CommandService {

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private ConcurrentMap<Class<?>, Set<Method>> commandHandlers;


  public Method getCommandHandler(Command command) {
    return CollectionUtils.emptyIfNull(commandHandlers.get(command.getPayload().getClass()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  public List<Event> invokeCommandHandler(Method methodToInvoke, Aggregate aggregate, Command command) {
    Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
    Object result;
    try {
      if (methodToInvoke.getParameterCount() == 2) {
        result = methodToInvoke.invoke(bean, aggregate != null ? aggregate.getPayload() : null, command.getPayload());
      } else {
        result = methodToInvoke.invoke(bean, aggregate != null ? aggregate.getPayload() : null, command.getPayload(), command.getMetadata());
      }
      return createEvents(command, result);

    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new CommandExecutionException(e.getCause().getMessage(), e.getCause());
    }
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

package com.github.easysourcing.message.events;

import com.github.easysourcing.message.Metadata;
import com.github.easysourcing.message.commands.Command;
import com.github.easysourcing.message.events.exceptions.EventProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
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
public class EventService {

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private ConcurrentMap<Class<?>, Set<Method>> eventHandlers;


  public Method getEventHandler(Event event) {
    return CollectionUtils.emptyIfNull(eventHandlers.get(event.getPayload().getClass()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  public List<Command> invokeEventHandler(Method methodToInvoke, Event event) {
    Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
    Object result;
    try {
      if (methodToInvoke.getParameterCount() == 1) {
        result = methodToInvoke.invoke(bean, event.getPayload());
      } else {
        result = methodToInvoke.invoke(bean, event.getPayload(), event.getMetadata());
      }
      return createCommands(result, event.getMetadata());

    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new EventProcessingException(e.getCause().getMessage(), e.getCause());
    }
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

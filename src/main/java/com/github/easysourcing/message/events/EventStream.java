package com.github.easysourcing.message.events;


import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.Metadata;
import com.github.easysourcing.message.commands.Command;
import com.github.easysourcing.message.events.exceptions.EventProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class EventStream {

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;

  @Autowired
  private KStream<String, Message> messageKStream;

  @Autowired
  private ConcurrentMap<String, Set<Method>> eventHandlers;

  @Autowired
  private ApplicationContext applicationContext;


  @Bean
  public KStream<String, Event> eventKStream() {

    // 1. Filter stream for Events
    KStream<String, Event> stream = messageKStream
        .filter((key, message) -> message instanceof Event)
        .mapValues((key, message) -> ((Event) message))
        .filter((key, event) -> event.getType() != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getAggregateId() != null);

    // 2. Process Events
    stream
        .filter((key, event) -> getEventHandler(event) != null)
        .peek((key, event) -> log.debug("Event received: {}", event))
        .mapValues(this::invokeEventHandler)
        .filter((key, commands) -> CollectionUtils.isNotEmpty(commands))
        .flatMapValues((ValueMapper<List<Command>, Iterable<Command>>) commands -> commands)
        .filter((key, command) -> command != null)
        .map((key, command) -> KeyValue.pair(command.getAggregateId(), command))
        .to(APPLICATION_ID.concat("-events"), Produced.with(Serdes.String(), new JsonSerde<>(Command.class).noTypeInfo()));

    return stream;
  }


  private Method getEventHandler(Event event) {
    return CollectionUtils.emptyIfNull(eventHandlers.get(event.getPayload().getClass().getName()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private List<Command> invokeEventHandler(Event event) {
    Method methodToInvoke = getEventHandler(event);
    if (methodToInvoke == null) {
      log.debug("No event-handler method found for event {}", event.getPayload().getClass().getSimpleName());
      return null;
    }

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

    return list.stream()
        .map(payload -> Command.builder()
            .type(payload.getClass().getSimpleName())
            .payload(payload)
            .metadata(metadata)
            .build())
        .collect(Collectors.toList());
  }
}

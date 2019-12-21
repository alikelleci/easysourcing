package com.github.easysourcing.message.commands;


import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.Metadata;
import com.github.easysourcing.message.commands.exceptions.CommandExecutionException;
import com.github.easysourcing.message.events.Event;
import com.github.easysourcing.message.snapshots.Snapshot;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
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
public class CommandStream {

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;

  @Autowired
  private KStream<String, Message> messageKStream;

  @Autowired
  private KTable<String, Snapshot> snapshotKTable;

  @Autowired
  private ConcurrentMap<String, Set<Method>> commandHandlers;

  @Autowired
  private ApplicationContext applicationContext;


  @Bean
  public KStream<String, Command> commandKStream() {

    // 1. Filter stream for Commands
    KStream<String, Command> stream = messageKStream
        .filter((key, message) -> message instanceof Command)
        .mapValues((key, message) -> ((Command) message))
        .filter((key, command) -> command.getType() != null)
        .filter((key, command) -> command.getPayload() != null)
        .filter((key, command) -> command.getAggregateId() != null);

    // 2. Process Commands
    stream
        .filter((key, command) -> getCommandHandler(command) != null)
        .peek((key, command) -> log.debug("Command received: {}", command))
        .leftJoin(snapshotKTable, this::invokeCommandHandler)
        .filter((key, events) -> CollectionUtils.isNotEmpty(events))
        .flatMapValues((ValueMapper<List<Event>, Iterable<Event>>) events -> events)
        .filter((key, event) -> event != null)
        .map((key, event) -> KeyValue.pair(event.getAggregateId(), event))
        .to(APPLICATION_ID.concat("-events"), Produced.with(Serdes.String(), new JsonSerde<>(Event.class).noTypeInfo()));

    return stream;
  }


  private Method getCommandHandler(Command command) {
    return CollectionUtils.emptyIfNull(commandHandlers.get(command.getPayload().getClass().getName()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private List<Event> invokeCommandHandler(Command command, Snapshot snapshot) {
    Method methodToInvoke = getCommandHandler(command);
    if (methodToInvoke == null) {
      log.debug("No command-handler method found for command {}", command.getPayload().getClass().getSimpleName());
      return null;
    }

    Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
    Object result;
    try {
      if (methodToInvoke.getParameterCount() == 2) {
        result = methodToInvoke.invoke(bean, snapshot != null ? snapshot.getPayload() : null, command.getPayload());
      } else {
        result = methodToInvoke.invoke(bean, snapshot != null ? snapshot.getPayload() : null, command.getPayload(), command.getMetadata());
      }
      return createEvents(result, command.getMetadata());

    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new CommandExecutionException(e.getCause().getMessage(), e.getCause());
    }
  }

  private List<Event> createEvents(Object result, Metadata metadata) {
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
        .map(payload -> Event.builder()
            .type(payload.getClass().getSimpleName())
            .payload(payload)
            .metadata(metadata)
            .build())
        .collect(Collectors.toList());
  }

}

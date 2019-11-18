package com.github.easysourcing.message.events;


import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.MessageType;
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
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

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
  public KStream<String, Message> eventKStream() {

    // 1. Filter stream for Events
    KStream<String, Message> stream = messageKStream
        .filter((key, message) -> message.getType() == MessageType.Event);


    // 2. Process Events
    KStream<String, Object>[] branches = stream
        .mapValues(Message::getPayload)
        .filter((key, payload) -> getEventHandler(payload) != null)
        .peek((key, payload) -> log.debug("Event received: {}", payload))
        .mapValues(this::invokeEventHandler)
        .filter((key, result) -> result != null)
        .branch(
            (key, result) -> !Collection.class.isAssignableFrom(result.getClass()),
            (key, result) -> Collection.class.isAssignableFrom(result.getClass())
        );


    // 2.1  Events resulted in a single Command
    branches[0]
        .mapValues((key, result) -> Message.builder()
            .type(MessageType.Command)
            .name(result.getClass().getSimpleName())
            .payload(result)
            .build())
        .map((key, message) -> KeyValue.pair(message.getAggregateId(), message))
        .to(APPLICATION_ID.concat("-events"), Produced.with(Serdes.String(), new JsonSerde<>(Message.class)));


    // 2.2  Events resulted in multiple Commands
    branches[1]
        .mapValues(result -> (Collection) result)
        .flatMapValues((ValueMapper<Collection, Iterable<?>>) collection -> collection)
        .filter((key, result) -> result != null)
        .mapValues((key, result) -> Message.builder()
            .type(MessageType.Command)
            .name(result.getClass().getSimpleName())
            .payload(result)
            .build())
        .map((key, message) -> KeyValue.pair(message.getAggregateId(), message))
        .to(APPLICATION_ID.concat("-events"), Produced.with(Serdes.String(), new JsonSerde<>(Message.class)));

    return stream;
  }


  private <E> Method getEventHandler(E payload) {
    return CollectionUtils.emptyIfNull(eventHandlers.get(payload.getClass().getName()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private <E> Object invokeEventHandler(E payload) {
    Method methodToInvoke = getEventHandler(payload);
    if (methodToInvoke == null) {
      log.debug("No event-handler method found for event {}", payload.getClass().getSimpleName());
      return null;
    }

    Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
    try {
      return methodToInvoke.invoke(bean, payload);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new EventProcessingException(e.getCause().getMessage(), e.getCause());
    }
  }


}

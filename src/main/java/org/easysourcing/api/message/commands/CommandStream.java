package org.easysourcing.api.message.commands;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.easysourcing.api.message.Message;
import org.easysourcing.api.message.MessageType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
public class CommandStream {

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;

  @Autowired
  private KStream<String, Message> messageKStream;

  @Autowired
  private Map<Class<?>, Set<Method>> commandHandlers;

  @Autowired
  private ApplicationContext applicationContext;


  @Bean
  public KStream<String, Message> commandKStream() {

    // 1. Filter stream for Commands
    KStream<String, Message> stream = messageKStream
        .filter((key, message) -> message.getType() == MessageType.Command);


    // 2. Process Commands
    KStream<String, Object>[] branches = stream
        .mapValues(Message::getPayload)
        .filter((key, payload) -> getCommandHandler(payload) != null)
        .mapValues(this::invokeCommandHandler)
        .filter((key, result) -> result != null)
        .branch(
            (key, result) -> !Collection.class.isAssignableFrom(result.getClass()),
            (key, result) -> Collection.class.isAssignableFrom(result.getClass())
        );


    // 2.1  Commands resulted in a single Event
    branches[0]
        .mapValues((key, result) -> Message.builder()
            .type(MessageType.Event)
            .payload(result)
            .build())
        .map((key, message) -> KeyValue.pair(message.getAggregateId(), message))
        .to("events." + APPLICATION_ID, Produced.with(Serdes.String(), new JsonSerde<>(Message.class)));


    // 2.2  Commands resulted in multiple Events
    branches[1]
        .mapValues(result -> (Collection) result)
        .flatMapValues((ValueMapper<Collection, Iterable<?>>) collection -> collection)
        .filter((key, result) -> result != null)
        .mapValues((key, result) -> Message.builder()
            .type(MessageType.Event)
            .payload(result)
            .build())
        .map((key, message) -> KeyValue.pair(message.getAggregateId(), message))
        .to("events." + APPLICATION_ID, Produced.with(Serdes.String(), new JsonSerde<>(Message.class)));

    return stream;
  }


  private <C> Method getCommandHandler(C payload) {
    return CollectionUtils.emptyIfNull(commandHandlers.get(payload.getClass()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private <C> Object invokeCommandHandler(C payload) {
    Method methodToInvoke = getCommandHandler(payload);
    if (methodToInvoke != null) {
      Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
      try {
        return MethodUtils.invokeExactMethod(bean, methodToInvoke.getName(), payload);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    log.debug("No command-handler method found for command {}", payload.getClass().getSimpleName());
    return null;
  }

}

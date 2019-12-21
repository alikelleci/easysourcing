package com.github.easysourcing.message.snapshots;


import com.github.easysourcing.message.events.Event;
import com.github.easysourcing.message.snapshots.exceptions.AggregateInvocationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.objenesis.Objenesis;
import org.springframework.objenesis.ObjenesisStd;
import org.springframework.objenesis.instantiator.ObjectInstantiator;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Component
public class SnapshotStream {

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;

  @Value("${spring.kafka.streams.replication-factor:1}")
  private int REPLICATION_FACTOR;

  @Autowired
  private KStream<String, Event> eventKStream;

  @Autowired
  private ConcurrentMap<String, Set<Method>> eventSourcingHandlers;

  @Autowired
  private ApplicationContext applicationContext;

  private Objenesis objenesis = new ObjenesisStd();


  @Bean
  public NewTopic snapshotsTopic() {
    return TopicBuilder.name(APPLICATION_ID.concat("-snapshots"))
        .partitions(3)
        .replicas(REPLICATION_FACTOR)
        .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "604800000") // 7 days
        .compact()
        .build();
  }

  @Bean
  public KTable<String, Snapshot> snapshotKTable(StreamsBuilder builder) {

    // 1. Process Events
    KTable<String, Snapshot> snapshotKTable = eventKStream
        .filter((key, event) -> getEventSourcingHandler(event) != null)
        .peek((key, event) -> log.debug("Event received: {}", event))
        .groupByKey()
        .aggregate(
            () -> Snapshot.builder().build(),
            (key, event, snapshot) -> doAggregate(event, snapshot),
            Materialized
                .<String, Snapshot, KeyValueStore<Bytes, byte[]>>as("snapshots")
                .withKeySerde(Serdes.String())
                .withValueSerde(new JsonSerde<>(Snapshot.class).noTypeInfo()));


    // 2. Send results to output stream
    snapshotKTable.toStream()
        .to(APPLICATION_ID.concat("-snapshots"), Produced.with(Serdes.String(), new JsonSerde<>(Snapshot.class).noTypeInfo()));


    // 3. Read snapshot from output stream into a Global Table to make it globally available for querying
//    builder.globalTable(APPLICATION_ID.concat("-snapshots"), Materialized
//        .<String, Snapshot, KeyValueStore<Bytes, byte[]>>as("snapshots-store")
//        .withKeySerde(Serdes.String())
//        .withValueSerde(new JsonSerde<>(Snapshot.class)));

    return snapshotKTable;
  }


  private Method getEventSourcingHandler(Event event) {
    return CollectionUtils.emptyIfNull(eventSourcingHandlers.get(event.getPayload().getClass().getName()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private Object invokeEventSourcingHandler(Event event, Snapshot snapshot) {
    Method methodToInvoke = getEventSourcingHandler(event);
    if (methodToInvoke == null) {
      log.debug("No event-sourcing-handler method found for event {}", event.getPayload().getClass().getSimpleName());
      return null;
    }

    Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
    try {
      if (methodToInvoke.getParameterCount() == 2) {
        return methodToInvoke.invoke(bean, snapshot.getPayload(), event.getPayload());
      } else {
        return methodToInvoke.invoke(bean, snapshot.getPayload(), event.getPayload(), event.getMetadata());
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new AggregateInvocationException(e.getCause().getMessage(), e.getCause());
    }
  }

  private <T> T instantiateClazz(Class<T> tClass) {
    ObjectInstantiator instantiator = objenesis.getInstantiatorOf(tClass);
    return (T) instantiator.newInstance();
  }

  private Snapshot doAggregate(Event event, Snapshot snapshot) {
    if (snapshot.getPayload() == null) {
      Object initialState = instantiateClazz(getEventSourcingHandler(event).getReturnType());
      snapshot = snapshot.toBuilder()
          .type(initialState.getClass().getSimpleName())
          .payload(initialState)
          .metadata(event.getMetadata())
          .build();
    }
    Object updatedState = invokeEventSourcingHandler(event, snapshot);
    return snapshot.toBuilder()
        .type(updatedState.getClass().getSimpleName())
        .payload(updatedState)
        .metadata(event.getMetadata())
        .build();
  }
}

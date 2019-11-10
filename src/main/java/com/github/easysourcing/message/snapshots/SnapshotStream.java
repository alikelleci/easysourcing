package com.github.easysourcing.message.snapshots;


import com.example.easysourcing.message.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Component
public class SnapshotStream {

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;

  @Autowired
  private KStream<String, Message> eventKStream;

  @Autowired
  private ConcurrentMap<String, Set<Method>> eventSourcingHandlers;

  @Autowired
  private ApplicationContext applicationContext;


  @Bean
  public NewTopic snapshotsTopic() {
    return TopicBuilder.name(APPLICATION_ID.concat("-snapshots"))
        .partitions(6)
        .replicas(1)
        .compact()
        .build();
  }

  @Bean
  public KTable<String, Snapshot> snapshotKTable(StreamsBuilder builder) {

    // 1. Process Events
    KTable<String, Snapshot> snapshotKTable = eventKStream
        .mapValues(Message::getPayload)
        .filter((key, payload) -> getEventSourcingHandler(payload) != null)
        .peek((key, payload) -> log.info("Event received: {}", payload))
        .groupByKey()
        .aggregate(
            () -> Snapshot.builder().build(),
            (key, payload, snapshot) -> doAggregate(payload, snapshot),
            Materialized
                .<String, Snapshot, KeyValueStore<Bytes, byte[]>>as("snapshots")
                .withKeySerde(Serdes.String())
                .withValueSerde(new JsonSerde<>(Snapshot.class)));


    // 2. Send results to output stream
    snapshotKTable.toStream()
        .to(APPLICATION_ID.concat("-snapshots"), Produced.with(Serdes.String(), new JsonSerde<>(Snapshot.class)));


    // 3. Read snapshot from output stream into a Global Table to make it globally available for querying
//    builder.globalTable(APPLICATION_ID.concat("-snapshots"), Materialized
//        .<String, Snapshot, KeyValueStore<Bytes, byte[]>>as("snapshots-store")
//        .withKeySerde(Serdes.String())
//        .withValueSerde(new JsonSerde<>(Snapshot.class)));

    return snapshotKTable;
  }


  private <E> Method getEventSourcingHandler(E payload) {
    return CollectionUtils.emptyIfNull(eventSourcingHandlers.get(payload.getClass().getName()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private <E, A> Object invokeEventSourcingHandler(E payload, A aggregate) {
    Method methodToInvoke = getEventSourcingHandler(payload);
    if (methodToInvoke != null) {
      Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
      try {
        return methodToInvoke.invoke(bean, payload, aggregate);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    log.debug("No event-handler method found for event {}", payload.getClass().getSimpleName());
    return null;
  }


  private Snapshot doAggregate(Object payload, Snapshot snapshot) {
    Object currentState = snapshot.getPayload();
    Object newState = invokeEventSourcingHandler(payload, currentState);
    return snapshot.toBuilder()
        .payload(newState)
        .build();
  }
}

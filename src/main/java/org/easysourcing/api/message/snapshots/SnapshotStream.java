package org.easysourcing.api.message.snapshots;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.easysourcing.api.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.objenesis.Objenesis;
import org.springframework.objenesis.ObjenesisStd;
import org.springframework.objenesis.instantiator.ObjectInstantiator;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SnapshotStream {

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;

  @Autowired
  private KStream<String, Message> eventKStream;

  @Autowired
  private Map<Class<?>, Set<Method>> eventSourcingHandlers;

  @Autowired
  private AdminClient adminClient;

  private Objenesis objenesis = new ObjenesisStd();


  @PostConstruct
  private void initTopics() throws ExecutionException, InterruptedException {
    List<String> existingTopics = adminClient.listTopics().listings().get()
        .stream()
        .map(TopicListing::name)
        .collect(Collectors.toList());


    Map<String, String> properties = new HashMap<>();
    properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    properties.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100");
    properties.put(TopicConfig.SEGMENT_MS_CONFIG, "100");
    properties.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01");


    List<String> topics = Arrays.asList("snapshots." + APPLICATION_ID);
    List<NewTopic> newTopics = new ArrayList<>();
    topics.forEach(topic ->
        newTopics.add(new NewTopic(topic, 6, (short) 1).configs(properties)));

    // filter existing topics and create new topics
    newTopics.removeIf(newTopic -> existingTopics.contains(newTopic.name()));
    adminClient.createTopics(newTopics).all().get();
  }

  @Bean
  public GlobalKTable<String, Snapshot> snapshotGlobalKTable(StreamsBuilder builder) {

    // 1. Process Events
    KTable<String, Snapshot> snapshots = eventKStream
        .mapValues(Message::getPayload)
        .filter((key, payload) -> getEventSourcingHandler(payload) != null)
        .groupByKey()
        .aggregate(
            Snapshot::new,
            (key, payload, snapshot) -> doAggregate(payload, snapshot),
            Materialized
                .<String, Snapshot, KeyValueStore<Bytes, byte[]>>as("snapshots")
                .withKeySerde(Serdes.String())
                .withValueSerde(new JsonSerde<>(Snapshot.class)));


    // 2. Send results to output stream
    snapshots.toStream()
        .to("snapshots." + APPLICATION_ID, Produced.with(Serdes.String(), new JsonSerde<>(Snapshot.class)));


    // 3. Read snapshot from output stream into a Global Table to make it available for query
    return builder.globalTable("snapshots." + APPLICATION_ID, Materialized
        .<String, Snapshot, KeyValueStore<Bytes, byte[]>>as("store")
        .withKeySerde(Serdes.String())
        .withValueSerde(new JsonSerde<>(Snapshot.class)));
  }


  private <E> Method getEventSourcingHandler(E payload) {
    return CollectionUtils.emptyIfNull(eventSourcingHandlers.get(payload.getClass()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private <E, A> A invokeEventSourcingHandler(E payload, A aggregate) {
    Method methodToInvoke = getEventSourcingHandler(payload);
    if (methodToInvoke != null) {
      try {
        return (A) MethodUtils.invokeExactMethod(aggregate, methodToInvoke.getName(), payload);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    log.debug("No event-handler method found for event {}", payload.getClass().getSimpleName());
    return null;
  }

  private <T> T instantiateClazz(Class<T> tClass) {
    ObjectInstantiator thingyInstantiator = objenesis.getInstantiatorOf(tClass);
    return (T) thingyInstantiator.newInstance();
  }

  private Snapshot doAggregate(Object payload, Snapshot snapshot) {
    Object aggregate = snapshot.getPayload();
    if (aggregate == null) {
      aggregate = instantiateClazz(getEventSourcingHandler(payload).getDeclaringClass());
    }
    aggregate = invokeEventSourcingHandler(payload, aggregate);
    return snapshot.toBuilder()
        .payload(aggregate)
        .build();
  }
}

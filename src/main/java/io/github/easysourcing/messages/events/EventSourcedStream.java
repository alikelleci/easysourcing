package io.github.easysourcing.messages.events;


import io.github.easysourcing.messages.aggregates.Aggregate;
import io.github.easysourcing.messages.aggregates.AggregateTransformer;
import io.github.easysourcing.messages.aggregates.Aggregator;
import io.github.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Slf4j
public class EventSourcedStream {

  private final Set<String> topics;
  private final Map<Class<?>, Aggregator> aggregators;
  private final boolean inMemoryStateStore;

  public EventSourcedStream(Set<String> topics, Map<Class<?>, Aggregator> aggregators, boolean inMemoryStateStore) {
    this.topics = topics;
    this.aggregators = aggregators;
    this.inMemoryStateStore = inMemoryStateStore;
  }

  public void buildStream(StreamsBuilder builder) {
    // Snapshot store
    KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore("snapshot-store");
    if (inMemoryStateStore) {
      supplier = Stores.inMemoryKeyValueStore("snapshot-store");
    }
    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            supplier,
            Serdes.String(),
            CustomSerdes.Json(Aggregate.class)
        ).withLoggingEnabled(Collections.emptyMap())
    );

    // --> Events
    KStream<String, Event> eventKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getAggregateId() != null);

    // Events --> Snapshots
    KStream<String, Aggregate> aggregateKStream = eventKStream
        .transformValues(() -> new AggregateTransformer(aggregators), "snapshot-store")
        .filter((key, aggregate) -> aggregate != null);

    // Snapshots Push
    aggregateKStream
        .to((key, snapshot, recordContext) -> snapshot.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Aggregate.class)));
  }

}

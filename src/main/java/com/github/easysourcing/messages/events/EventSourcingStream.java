package com.github.easysourcing.messages.events;


import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Slf4j
public class EventSourcingStream {

  private final Set<String> topics;
  private final Map<Class<?>, Aggregator> aggregators;

  public EventSourcingStream(Set<String> topics, Map<Class<?>, Aggregator> aggregators) {
    this.topics = topics;
    this.aggregators = aggregators;
  }

  public void buildStream(StreamsBuilder builder) {
    // Snapshot store
    builder.addStateStore(
        Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore("snapshot-store"),
            Serdes.String(),
            CustomSerdes.Json(Aggregate.class)
        ).withLoggingEnabled(Collections.emptyMap())
    );

    // --> Events
    KStream<String, Event> eventsKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getAggregateId() != null);

    // Events --> Snapshots
    KStream<String, Aggregate> snapshotsKStream = eventsKStream
        .transformValues(() -> new EventSourcingTransformer(aggregators), "snapshot-store")
        .filter((key, snapshot) -> snapshot != null);

    // Snapshots Push
    snapshotsKStream
        .to((key, snapshot, recordContext) -> snapshot.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Aggregate.class)));

  }

}

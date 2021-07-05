package io.github.alikelleci.easysourcing.messages.eventsourcing;


import io.github.alikelleci.easysourcing.OperationMode;
import io.github.alikelleci.easysourcing.messages.events.Event;
import io.github.alikelleci.easysourcing.messages.snapshots.Snapshot;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Set;

@Slf4j
public class EventSourcingStream {

  private final Set<String> topics;
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;
  private final OperationMode operationMode;

  public EventSourcingStream(Set<String> topics, Map<Class<?>, EventSourcingHandler> eventSourcingHandlers, OperationMode operationMode) {
    this.topics = topics;
    this.eventSourcingHandlers = eventSourcingHandlers;
    this.operationMode = operationMode;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Events
    KStream<String, Event> events = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getAggregateId() != null);

    // Events --> Snapshots
    KStream<String, Snapshot> snapshots = events
        .transformValues(() -> new EventSourcingTransformer(eventSourcingHandlers), "snapshot-store")
        .filter((key, snapshot) -> snapshot != null);

    // Snapshots Push
    if (operationMode == OperationMode.EVENT_SOURCED_PUBLISH) {
      snapshots
          .to((key, snapshot, recordContext) -> snapshot.getTopicInfo().value(),
              Produced.with(Serdes.String(), CustomSerdes.Json(Snapshot.class)));
    }
  }

}

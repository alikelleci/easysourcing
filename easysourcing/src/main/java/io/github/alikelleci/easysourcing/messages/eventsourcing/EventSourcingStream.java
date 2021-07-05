package io.github.alikelleci.easysourcing.messages.eventsourcing;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.OperationMode;
import io.github.alikelleci.easysourcing.messages.MessageTransformer;
import io.github.alikelleci.easysourcing.messages.events.Event;
import io.github.alikelleci.easysourcing.messages.snapshots.Snapshot;
import io.github.alikelleci.easysourcing.messages.upcasters.PayloadTransformer;
import io.github.alikelleci.easysourcing.messages.upcasters.Upcaster;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
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
  private final MultiValuedMap<String, Upcaster> upcasters;
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;
  private final OperationMode operationMode;

  public EventSourcingStream(Set<String> topics, MultiValuedMap<String, Upcaster> upcasters, Map<Class<?>, EventSourcingHandler> eventSourcingHandlers, OperationMode operationMode) {
    this.topics = topics;
    this.upcasters = upcasters;
    this.eventSourcingHandlers = eventSourcingHandlers;
    this.operationMode = operationMode;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Events --> Snapshots
    KStream<String, Snapshot> snapshots = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        // Filter
        .filter((key, value) -> key != null)
        .filter((key, value) -> value != null)

        // Upcast & convert
        .transformValues(() -> new PayloadTransformer(upcasters))
        .transformValues(() -> new MessageTransformer<>(Event.class))

        // Filter
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getAggregateId() != null)

        // Invoke handlers
        .transformValues(() -> new EventSourcingTransformer(eventSourcingHandlers), "snapshot-store")

        // Filter
        .filter((key, snapshot) -> snapshot != null)
        .filter((key, snapshot) -> snapshot.getPayload() != null)
        .filter((key, snapshot) -> snapshot.getTopicInfo() != null)
        .filter((key, snapshot) -> snapshot.getAggregateId() != null);

    // Snapshots Push
    if (operationMode == OperationMode.EVENT_SOURCED_PUBLISH) {
      snapshots
          .to((key, snapshot, recordContext) -> snapshot.getTopicInfo().value(),
              Produced.with(Serdes.String(), CustomSerdes.Json(Snapshot.class)));
    }
  }

}

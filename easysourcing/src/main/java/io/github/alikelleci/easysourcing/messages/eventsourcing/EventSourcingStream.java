package io.github.alikelleci.easysourcing.messages.eventsourcing;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.upcasters.PayloadTransformer;
import io.github.alikelleci.easysourcing.messages.upcasters.Upcaster;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Map;
import java.util.Set;

@Slf4j
public class EventSourcingStream {

  private final Set<String> topics;
  private final MultiValuedMap<String, Upcaster> upcasters;
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;

  public EventSourcingStream(Set<String> topics, MultiValuedMap<String, Upcaster> upcasters, Map<Class<?>, EventSourcingHandler> eventSourcingHandlers) {
    this.topics = topics;
    this.upcasters = upcasters;
    this.eventSourcingHandlers = eventSourcingHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Events
    KStream<String, JsonNode> events = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null);

    // Events --> Snapshots
    KStream<String, Object> snapshots = events
        .transformValues(() -> new PayloadTransformer(upcasters))
        .transformValues(() -> new EventSourcingTransformer(eventSourcingHandlers), "snapshots");

  }

}

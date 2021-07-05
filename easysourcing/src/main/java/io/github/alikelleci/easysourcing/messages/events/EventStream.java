package io.github.alikelleci.easysourcing.messages.events;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.upcasters.UpcastTransformer;
import io.github.alikelleci.easysourcing.messages.upcasters.Upcaster;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import io.github.alikelleci.easysourcing.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Set;

@Slf4j
public class EventStream {

  private final Set<String> topics;
  private final MultiValuedMap<String, Upcaster> upcasters;
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers;


  public EventStream(Set<String> topics, MultiValuedMap<String, Upcaster> upcasters, MultiValuedMap<Class<?>, EventHandler> eventHandlers) {
    this.topics = topics;
    this.upcasters = upcasters;
    this.eventHandlers = eventHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Events
    KStream<String, Event> events = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, value) -> key != null)
        .filter((key, value) -> value != null)
        .transformValues(() -> new UpcastTransformer(upcasters))
        .mapValues(value -> JacksonUtils.enhancedObjectMapper().convertValue(value, Event.class))

        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getAggregateId() != null);

    // Events --> Void
    events
        .transformValues(() -> new EventTransformer(eventHandlers));
  }

}

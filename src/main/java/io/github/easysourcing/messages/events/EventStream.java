package io.github.easysourcing.messages.events;


import io.github.easysourcing.support.serializer.CustomSerdes;
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
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers;

  public EventStream(Set<String> topics, MultiValuedMap<Class<?>, EventHandler> eventHandlers) {
    this.topics = topics;
    this.eventHandlers = eventHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Events
    KStream<String, Event> eventKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getAggregateId() != null);

    // Events --> Void
    eventKStream
        .transformValues(() -> new EventTransformer(eventHandlers));
  }

}

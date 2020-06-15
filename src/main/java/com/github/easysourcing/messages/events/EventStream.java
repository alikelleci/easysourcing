package com.github.easysourcing.messages.events;


import com.github.easysourcing.serdes.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class EventStream {

  private final Set<String> topics;
  private final ConcurrentMap<Class<?>, EventHandler> eventHandlers;
  private final boolean frequentCommits;

  public EventStream(Set<String> topics, ConcurrentMap<Class<?>, EventHandler> eventHandlers, boolean frequentCommits) {
    this.topics = topics;
    this.eventHandlers = eventHandlers;
    this.frequentCommits = frequentCommits;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Events
    KStream<String, Event> eventKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), new CustomJsonSerde<>(Event.class).noTypeInfo()))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getAggregateId() != null);

    // Events --> Void
    eventKStream
        .transformValues(() -> new EventTransformer(eventHandlers, frequentCommits));
  }

}

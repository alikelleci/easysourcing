package com.github.easysourcing.messages.events;


import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.serdes.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;
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

  public KStream<String, Event> buildStream(StreamsBuilder builder) {
    // --> Events
    KStream<String, Event> eventKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), new CustomJsonSerde<>(Message.class).noTypeInfo()))
        .filter((key, message) -> key != null)
        .filter((key, message) -> message != null)
        .filter((key, message) -> message.getUuid() != null)
        .filter((key, message) -> message.getAggregateId() != null)
        .filter((key, message) -> message.getPayload() != null)
        .filter((key, message) -> message.getTopicInfo() != null)
        .filter((key, message) -> message instanceof Event)
        .mapValues((key, message) -> (Event) message);

    // Events --> Commands
    eventKStream
        .transformValues(() -> new EventTransformer(eventHandlers, frequentCommits))
        .filter((key, commands) -> CollectionUtils.isNotEmpty(commands))
        .flatMapValues((ValueMapper<List<Command>, Iterable<Command>>) commands -> commands)
        .filter((key, command) -> command != null)
        .map((key, command) -> KeyValue.pair(command.getAggregateId(), command))
        .to((key, command, recordContext) -> command.getTopicInfo().value(),
            Produced.with(Serdes.String(), new JsonSerde<>(Command.class).noTypeInfo()));

    return eventKStream;
  }

}

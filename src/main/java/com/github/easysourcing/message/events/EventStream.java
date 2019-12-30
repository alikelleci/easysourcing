package com.github.easysourcing.message.events;


import com.github.easysourcing.message.commands.Command;
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

  public EventStream(Set<String> topics, ConcurrentMap<Class<?>, EventHandler> eventHandlers) {
    this.topics = topics;
    this.eventHandlers = eventHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    KStream<String, Event> eventKStream = builder
        .stream(topics,
            Consumed.with(Serdes.String(), new CustomJsonSerde<>(Event.class).noTypeInfo()))
//        .peek((key, event) -> log.debug("Message received: {}", event))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getId() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getType() != null)
        .filter((key, event) -> event.getPayload() != null);

    KStream<String, Command> commandKStream = eventKStream
        .transformValues(() -> new EventTransformer(eventHandlers))
        .filter((key, commands) -> CollectionUtils.isNotEmpty(commands))
        .flatMapValues((ValueMapper<List<Command>, Iterable<Command>>) commands -> commands)
        .filter((key, command) -> command != null)
        .map((key, command) -> KeyValue.pair(command.getId(), command));

    commandKStream
        .to((key, command, recordContext) -> command.getTopicInfo().value(),
            Produced.with(Serdes.String(), new JsonSerde<>(Command.class).noTypeInfo()));
  }

}

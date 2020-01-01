package com.github.easysourcing.message.events;


import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.commands.Command;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class EventStream {

  private final ConcurrentMap<Class<?>, EventHandler> eventHandlers;

  public EventStream(ConcurrentMap<Class<?>, EventHandler> eventHandlers) {
    this.eventHandlers = eventHandlers;
  }

  public KStream<String, Event> buildStream(KStream<String, Message> messageKStream) {
    // --> Events
    KStream<String, Event> eventKStream = messageKStream
        .filter((key, message) -> message instanceof Event)
        .mapValues((key, message) -> (Event) message);

    // Events --> Commands
    eventKStream
        .transformValues(() -> new EventTransformer(eventHandlers))
        .filter((key, commands) -> CollectionUtils.isNotEmpty(commands))
        .flatMapValues((ValueMapper<List<Command>, Iterable<Command>>) commands -> commands)
        .filter((key, command) -> command != null)
        .map((key, command) -> KeyValue.pair(command.getId(), command))
        .to((key, command, recordContext) -> command.getTopicInfo().value(),
            Produced.with(Serdes.String(), new JsonSerde<>(Command.class).noTypeInfo()));

    return eventKStream;
  }

}

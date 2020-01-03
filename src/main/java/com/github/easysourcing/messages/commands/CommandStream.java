package com.github.easysourcing.messages.commands;


import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.events.Event;
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
public class CommandStream {

  private final ConcurrentMap<Class<?>, CommandHandler> commandHandlers;
  private final ConcurrentMap<Class<?>, Aggregator> aggregators;

  public CommandStream(ConcurrentMap<Class<?>, CommandHandler> commandHandlers,
                       ConcurrentMap<Class<?>, Aggregator> aggregators) {
    this.commandHandlers = commandHandlers;
    this.aggregators = aggregators;
  }

  public KStream<String, Command> buildStream(KStream<String, Message> messageKStream) {
    // --> Commands
    KStream<String, Command> commandKStream = messageKStream
        .filter((key, message) -> message instanceof Command)
        .mapValues((key, message) -> (Command) message);

    // Commands --> Events
    commandKStream
        .transformValues(() -> new CommandTransformer(commandHandlers, aggregators), "snapshot-store")
        .filter((key, events) -> CollectionUtils.isNotEmpty(events))
        .flatMapValues((ValueMapper<List<Event>, Iterable<Event>>) events -> events)
        .filter((key, event) -> event != null)
        .map((key, event) -> KeyValue.pair(event.getId(), event))
        .to((key, event, recordContext) -> event.getTopicInfo().value(),
            Produced.with(Serdes.String(), new JsonSerde<>(Event.class).noTypeInfo()));

    return commandKStream;
  }

}

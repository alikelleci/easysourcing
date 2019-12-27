package com.github.easysourcing.message.events;


import com.github.easysourcing.kafka.streams.serdes.CustomJsonSerde;
import com.github.easysourcing.message.commands.Command;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Slf4j
@Component
public class EventStream {

  @Autowired
  private Set<String> eventsTopics;

  @Autowired
  private EventService eventService;


  @Bean
  public KStream<String, Event> eventKStream(StreamsBuilder builder) {
    if(eventsTopics.isEmpty()) {
      return null;
    }

    KStream<String, Event> eventKStream = builder
        .stream(eventsTopics,
            Consumed.with(Serdes.String(), new CustomJsonSerde<>(Event.class).noTypeInfo()))
//        .peek((key, event) -> log.debug("Message received: {}", event))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getId() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getType() != null)
        .filter((key, event) -> event.getPayload() != null);

    KStream<String, Command> commandKStream = eventKStream
        .transformValues(() -> new EventInvoker(eventService))
        .filter((key, commands) -> CollectionUtils.isNotEmpty(commands))
        .flatMapValues((ValueMapper<List<Command>, Iterable<Command>>) commands -> commands)
        .filter((key, command) -> command != null)
        .map((key, command) -> KeyValue.pair(command.getId(), command));

    commandKStream
        .to((key, command, recordContext) -> command.getTopicInfo().value(),
            Produced.with(Serdes.String(), new JsonSerde<>(Command.class).noTypeInfo()));

    return eventKStream;
  }

}

package com.github.easysourcing.message.commands;


import com.github.easysourcing.kafka.streams.serdes.CustomJsonSerde;
import com.github.easysourcing.message.aggregates.Aggregate;
import com.github.easysourcing.message.aggregates.AggregateService;
import com.github.easysourcing.message.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Set;

@Slf4j
@Component
public class CommandStream {

  @Autowired
  private Set<String> commandsTopics;

  @Autowired
  private CommandService commandService;

  @Autowired
  private AggregateService aggregateService;


  @Bean
  public KStream<String, Command> commandKStream(StreamsBuilder builder) {
    if (commandsTopics.isEmpty()) {
      return null;
    }

    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("snapshots"), Serdes.String(), new JsonSerde<>(Aggregate.class).noTypeInfo())
        .withLoggingEnabled(Collections.singletonMap(TopicConfig.DELETE_RETENTION_MS_CONFIG, "604800000"))); // 7 days

    KStream<String, Command> commandKStream = builder
        .stream(commandsTopics,
            Consumed.with(Serdes.String(), new CustomJsonSerde<>(Command.class).noTypeInfo()))
//        .peek((key, command) -> log.debug("Message received: {}", command))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null)
        .filter((key, command) -> command.getId() != null)
        .filter((key, command) -> command.getTopicInfo() != null)
        .filter((key, command) -> command.getType() != null)
        .filter((key, command) -> command.getPayload() != null);

    KStream<String, Event> eventKStream = commandKStream
        .transformValues(() -> new CommandInvoker(commandService, aggregateService), "snapshots")
        .filter((key, events) -> CollectionUtils.isNotEmpty(events))
        .flatMapValues((ValueMapper<List<Event>, Iterable<Event>>) events -> events)
        .filter((key, event) -> event != null)
        .map((key, event) -> KeyValue.pair(event.getId(), event));

    eventKStream
        .to((key, event, recordContext) -> event.getTopicInfo().value(),
            Produced.with(Serdes.String(), new JsonSerde<>(Event.class).noTypeInfo()));

    return commandKStream;
  }

}

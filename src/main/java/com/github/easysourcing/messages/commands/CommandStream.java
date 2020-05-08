package com.github.easysourcing.messages.commands;


import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.events.Event;
import com.github.easysourcing.serdes.CustomJsonSerde;
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
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class CommandStream {

  private final Set<String> topics;
  private final ConcurrentMap<Class<?>, CommandHandler> commandHandlers;
  private final ConcurrentMap<Class<?>, Aggregator> aggregators;
  private final boolean frequentCommits;

  public CommandStream(Set<String> topics,
                       ConcurrentMap<Class<?>, CommandHandler> commandHandlers,
                       ConcurrentMap<Class<?>, Aggregator> aggregators, boolean frequentCommits) {
    this.topics = topics;
    this.commandHandlers = commandHandlers;
    this.aggregators = aggregators;
    this.frequentCommits = frequentCommits;
  }

  public KStream<String, Command> buildStream(StreamsBuilder builder) {
    // Snapshot store
    builder.addStateStore(
        Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore("snapshot-store"),
            Serdes.String(),
            new JsonSerde<>(Aggregate.class).noTypeInfo()
        ).withLoggingEnabled(Collections.singletonMap(TopicConfig.DELETE_RETENTION_MS_CONFIG, "1209600000")) // 14 days
    );

    // --> Commands
    KStream<String, Command> commandKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), new CustomJsonSerde<>(Message.class).noTypeInfo()))
        .filter((key, message) -> key != null)
        .filter((key, message) -> message != null)
        .filter((key, message) -> message.getUuid() != null)
        .filter((key, message) -> message.getAggregateId() != null)
        .filter((key, message) -> message.getPayload() != null)
        .filter((key, message) -> message.getTopicInfo() != null)
        .filter((key, message) -> message instanceof Command)
        .mapValues((key, message) -> (Command) message);

    // Commands --> Events
    commandKStream
        .transformValues(() -> new CommandTransformer(commandHandlers, aggregators, frequentCommits), "snapshot-store")
        .filter((key, events) -> CollectionUtils.isNotEmpty(events))
        .flatMapValues((ValueMapper<List<Event>, Iterable<Event>>) events -> events)
        .filter((key, event) -> event != null)
        .map((key, event) -> KeyValue.pair(event.getAggregateId(), event))
        .to((key, event, recordContext) -> event.getTopicInfo().value(),
            Produced.with(Serdes.String(), new JsonSerde<>(Event.class).noTypeInfo()));

    return commandKStream;
  }

}

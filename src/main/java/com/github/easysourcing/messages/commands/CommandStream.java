package com.github.easysourcing.messages.commands;


import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.commands.CommandResult.Success;
import com.github.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Slf4j
public class CommandStream {

  private final Set<String> topics;
  private final Map<Class<?>, CommandHandler> commandHandlers;
  private final Map<Class<?>, Aggregator> aggregators;

  public CommandStream(Set<String> topics, Map<Class<?>, CommandHandler> commandHandlers, Map<Class<?>, Aggregator> aggregators) {
    this.topics = topics;
    this.commandHandlers = commandHandlers;
    this.aggregators = aggregators;
  }

  public void buildStream(StreamsBuilder builder) {
    // Snapshot store
    builder.addStateStore(
        Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore("snapshot-store"),
            Serdes.String(),
            CustomSerdes.Aggregate()
        ).withLoggingEnabled(Collections.emptyMap())
    );

    // --> Commands
    KStream<String, Command> commandsKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), CustomSerdes.Command()))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null)
        .filter((key, command) -> command.getPayload() != null)
        .filter((key, command) -> command.getTopicInfo() != null)
        .filter((key, command) -> command.getAggregateId() != null);

    // Commands --> Results
    KStream<String, CommandResult> resultsKStream = commandsKStream
        .transformValues(() -> new CommandTransformer(commandHandlers, aggregators), "snapshot-store")
        .filter((key, result) -> result != null)
        .filter((key, result) -> result.getCommand() != null);

    // Results --> Push
    resultsKStream
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> command.getTopicInfo().value().concat(".results"),
            Produced.with(Serdes.String(), CustomSerdes.Command()));

    // Results --> Success
    KStream<String, Success> successKStream = resultsKStream
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result);

    // Success --> Snapshots Push
    successKStream
        .mapValues(Success::getSnapshot)
        .filter((key, snapshot) -> snapshot != null)
        .to((key, snapshot, recordContext) -> snapshot.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Aggregate()));

    // Success --> Events Push
    successKStream
        .mapValues(Success::getEvents)
        .filter((key, events) -> events != null)
        .flatMapValues((key, events) -> events)
        .filter((key, event) -> event != null)
        .map((key, event) -> KeyValue.pair(event.getAggregateId(), event))
        .to((key, event, recordContext) -> event.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Event()));
  }

}

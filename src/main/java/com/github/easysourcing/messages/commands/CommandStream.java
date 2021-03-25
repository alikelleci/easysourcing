package com.github.easysourcing.messages.commands;


import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.aggregates.AggregateTransformer;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.commands.CommandResult.Success;
import com.github.easysourcing.messages.events.Event;
import com.github.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
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

  public void buildStream(StreamsBuilder builder, boolean inMemoryStateStore) {
    // Snapshot store
    KeyValueBytesStoreSupplier supplier = Stores.persistentTimestampedKeyValueStore("snapshot-store");
    if (inMemoryStateStore) {
      supplier = Stores.inMemoryKeyValueStore("snapshot-store");
    }

    builder.addStateStore(
        Stores.timestampedKeyValueStoreBuilder(
            supplier,
            Serdes.String(),
            CustomSerdes.Json(Aggregate.class)
        ).withLoggingEnabled(Collections.emptyMap())
    );

    // --> Commands
    KStream<String, Command> commandKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null)
        .filter((key, command) -> command.getPayload() != null)
        .filter((key, command) -> command.getTopicInfo() != null)
        .filter((key, command) -> command.getAggregateId() != null);

    // Commands --> Results
    KStream<String, CommandResult> resultKStream = commandKStream
        .transformValues(() -> new CommandTransformer(commandHandlers), "snapshot-store")
        .filter((key, result) -> result != null)
        .filter((key, result) -> result.getCommand() != null);

    // Results --> Events
    KStream<String, Event> eventKStream = resultKStream
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result)
        .flatMapValues(Success::getEvents)
        .filter((key, event) -> event != null);

    // Events --> Snapshots
    KStream<String, Aggregate> aggregateKStream = eventKStream
        .transformValues(() -> new AggregateTransformer(aggregators), "snapshot-store")
        .filter((key, aggregate) -> aggregate != null);

    // Results --> Push
    resultKStream
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> command.getTopicInfo().value().concat(".results"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

    // Events --> Push
    eventKStream
        .to((key, event, recordContext) -> event.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));

    // Snapshots --> Push
    aggregateKStream
        .to((key, aggregate, recordContext) -> aggregate.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Aggregate.class)));

  }

}

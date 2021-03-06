package io.github.alikelleci.easysourcing.messages.commands;


import io.github.alikelleci.easysourcing.messages.aggregates.Aggregate;
import io.github.alikelleci.easysourcing.messages.aggregates.AggregateTransformer;
import io.github.alikelleci.easysourcing.messages.aggregates.Aggregator;
import io.github.alikelleci.easysourcing.messages.events.Event;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Slf4j
public class CommandStream {

  private final Set<String> topics;
  private final Map<Class<?>, CommandHandler> commandHandlers;
  private final Map<Class<?>, Aggregator> aggregators;
  private final boolean inMemoryStateStore;

  public CommandStream(Set<String> topics, Map<Class<?>, CommandHandler> commandHandlers, Map<Class<?>, Aggregator> aggregators, boolean inMemoryStateStore) {
    this.topics = topics;
    this.commandHandlers = commandHandlers;
    this.aggregators = aggregators;
    this.inMemoryStateStore = inMemoryStateStore;
  }

  public void buildStream(StreamsBuilder builder) {
    // Snapshot store
    KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore("snapshot-store");
    if (inMemoryStateStore) {
      supplier = Stores.inMemoryKeyValueStore(supplier.name());
    }
    StoreBuilder storeBuilder = Stores
        .keyValueStoreBuilder(supplier, Serdes.String(), CustomSerdes.Json(Aggregate.class))
        .withLoggingEnabled(Collections.emptyMap());
    builder.addStateStore(storeBuilder);

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
        .transformValues(() -> new CommandTransformer(commandHandlers), supplier.name())
        .filter((key, result) -> result != null)
        .filter((key, result) -> result.getCommand() != null);

    // Results --> Events
    KStream<String, Event> eventKStream = resultKStream
        .filter((key, result) -> result instanceof CommandResult.Success)
        .mapValues((key, result) -> (CommandResult.Success) result)
        .flatMapValues(CommandResult.Success::getEvents)
        .filter((key, event) -> event != null);

    // Events --> Snapshots
    KStream<String, Aggregate> aggregateKStream = eventKStream
        .transformValues(() -> new AggregateTransformer(aggregators), supplier.name())
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

package io.github.alikelleci.easysourcing.core.commands;


import io.github.alikelleci.easysourcing.core.aggregates.Aggregate;
import io.github.alikelleci.easysourcing.core.aggregates.AggregateTransformer;
import io.github.alikelleci.easysourcing.core.aggregates.Aggregator;
import io.github.alikelleci.easysourcing.core.commands.CommandResult.Failure;
import io.github.alikelleci.easysourcing.core.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.core.events.Event;
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
    KStream<String, Command> commands = builder.stream(topics,
        Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null)
        .filter((key, command) -> command.getPayload() != null)
        .filter((key, command) -> command.getTopicInfo() != null)
        .filter((key, command) -> command.getAggregateId() != null);

    // Commands --> Command results
    KStream<String, CommandResult> commandResults = commands
        .transformValues(() -> new CommandTransformer(commandHandlers), supplier.name())
        .filter((key, result) -> result != null)
        .filter((key, result) -> result.getCommand() != null);

    // Command results --> Successful commands
    KStream<String, Success> successfulCommands = commandResults
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result);

    // Command results --> Failed commands
    KStream<String, Failure> failedCommands = commandResults
        .filter((key, result) -> result instanceof Failure)
        .mapValues((key, result) -> (Failure) result);

    // Successful commands --> Events
    KStream<String, Event> events = successfulCommands
        .flatMapValues(Success::getEvents)
        .filter((key, event) -> event != null);

    // Events --> Snapshots
    KStream<String, Aggregate> snapshots = events
        .transformValues(() -> new AggregateTransformer(aggregators), supplier.name())
        .filter((key, aggregate) -> aggregate != null);

    // Events --> Push
    events
        .to((key, event, recordContext) -> event.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));

    // Snapshots --> Push
    snapshots
        .to((key, aggregate, recordContext) -> aggregate.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Aggregate.class)));


    // Failed commands --> Push
    failedCommands
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> command.getTopicInfo().value().concat(".exceptions"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

  }

}

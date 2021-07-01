package io.github.alikelleci.easysourcing.messages.commands;


import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Failure;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.messages.events.Event;
import io.github.alikelleci.easysourcing.messages.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.easysourcing.messages.eventsourcing.EventSourcingTransformer;
import io.github.alikelleci.easysourcing.messages.snapshots.Snapshot;
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
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandler;
  private final boolean inMemoryStateStore;

  public CommandStream(Set<String> topics, Map<Class<?>, CommandHandler> commandHandlers, Map<Class<?>, EventSourcingHandler> eventSourcingHandler, boolean inMemoryStateStore) {
    this.topics = topics;
    this.commandHandlers = commandHandlers;
    this.eventSourcingHandler = eventSourcingHandler;
    this.inMemoryStateStore = inMemoryStateStore;
  }

  public void buildStream(StreamsBuilder builder) {
    // Snapshot store
    KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore("snapshot-store");
    if (inMemoryStateStore) {
      supplier = Stores.inMemoryKeyValueStore(supplier.name());
    }
    StoreBuilder storeBuilder = Stores
        .keyValueStoreBuilder(supplier, Serdes.String(), CustomSerdes.Json(Snapshot.class))
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
    KStream<String, Snapshot> snapshots = events
        .transformValues(() -> new EventSourcingTransformer(eventSourcingHandler), supplier.name())
        .filter((key, snapshot) -> snapshot != null);

    // Events --> Push
    events
        .to((key, event, recordContext) -> event.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));

    // Snapshots --> Push
    snapshots
        .to((key, snapshot, recordContext) -> snapshot.getTopicInfo().value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Snapshot.class)));


    // Failed commands --> Push
    failedCommands
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> command.getTopicInfo().value().concat(".exceptions"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

  }

}

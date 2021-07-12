package io.github.alikelleci.easysourcing.messages.commands;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.RevisionAdder;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Error;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.messages.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.easysourcing.messages.eventsourcing.EventSourcingTransformer;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Slf4j
public class CommandStream {

  private final Set<String> topics;
  private final Map<Class<?>, CommandHandler> commandHandlers;
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;

  public CommandStream(Set<String> topics, Map<Class<?>, CommandHandler> commandHandlers, Map<Class<?>, EventSourcingHandler> eventSourcingHandlers) {
    this.topics = topics;
    this.commandHandlers = commandHandlers;
    this.eventSourcingHandlers = eventSourcingHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // Snapshots state store
    StoreBuilder storeBuilder = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("snapshots"), Serdes.String(), CustomSerdes.Json(JsonNode.class))
        .withLoggingEnabled(Collections.emptyMap());

    builder.addStateStore(storeBuilder);

    // --> Commands
    KStream<String, JsonNode> commands = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);

    // Commands --> Command results
    KStream<String, CommandResult> commandResults = commands
        .transformValues(() -> new CommandTransformer(commandHandlers), "snapshots")
        .filter((key, result) -> result != null);

    // Successful results --> Events
    KStream<String, Object> events = commandResults
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result)
        .flatMapValues(Success::getEvents);

    // Error results --> Error commands
    KStream<String, Object> failedCommands = commandResults
        .filter((key, result) -> result instanceof Error)
        .mapValues((key, result) -> (Error) result)
        .mapValues(CommandResult::getCommand);

    // Events --> Snapshots
    KStream<String, Object> snapshots = events
        .mapValues(JsonUtils::toJsonNode)
        .transformValues(() -> new EventSourcingTransformer(eventSourcingHandlers), "snapshots");

    // Events --> Push
    events
        .transformValues(RevisionAdder::new)
        .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Snapshots --> Push
    snapshots
        .to((key, snapshot, recordContext) -> CommonUtils.getTopicInfo(snapshot).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Error commands --> Push
    failedCommands
        .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command).value().concat(".errors"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

  }

}

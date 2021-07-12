package io.github.alikelleci.easysourcing.messages.commands;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.OperationMode;
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

import static io.github.alikelleci.easysourcing.EasySourcingBuilder.APPLICATION_ID;
import static io.github.alikelleci.easysourcing.EasySourcingBuilder.OPERATION_MODE;

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
    // Stores
    StoreBuilder storeBuilder1 = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("command-redirects"), Serdes.String(), Serdes.Long())
        .withLoggingEnabled(Collections.emptyMap());

    StoreBuilder storeBuilder2 = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("snapshots"), Serdes.String(), CustomSerdes.Json(JsonNode.class))
        .withLoggingEnabled(Collections.emptyMap());

    builder.addStateStore(storeBuilder1);
    builder.addStateStore(storeBuilder2);

    if (OPERATION_MODE == OperationMode.RETRY) {
      topics.clear();
      topics.add(APPLICATION_ID.concat(".commands-retry"));
    }

    // --> Commands
    KStream<String, JsonNode> commands = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);

    // Commands --> Results
    KStream<String, Object>[] results = commands
        .transformValues(() -> new CommandTransformer(commandHandlers), "command-redirects", "snapshots")
        .branch(
            (key, value) -> value instanceof Success, // processed
            (key, value) -> value instanceof Error,   // processed
            (key, value) -> true                      // not processed
        );

    // Successful results --> Events
    KStream<String, Object> events = results[0]
        .mapValues((key, result) -> (Success) result)
        .flatMapValues(Success::getEvents)
        .filter((key, event) -> event != null)
        .filter((key, event) -> CommonUtils.getTopicInfo(event) != null)
        .filter((key, event) -> CommonUtils.getAggregateId(event) != null);

    // Failed results --> Failed commands
    KStream<String, Object> failedCommands = results[1]
        .mapValues((key, result) -> (Error) result)
        .mapValues(CommandResult::getCommand)
        .filter((key, command) -> command != null)
        .filter((key, command) -> CommonUtils.getTopicInfo(command) != null)
        .filter((key, command) -> CommonUtils.getAggregateId(command) != null);

    // Unprocessed results --> Unprocessed commands
    KStream<String, Object> unprocessed = results[2]
        .filter((key, command) -> command != null)
        .filter((key, command) -> CommonUtils.getTopicInfo(command) != null)
        .filter((key, command) -> CommonUtils.getAggregateId(command) != null);

    // Events --> Snapshots
    KStream<String, Object> snapshots = events
        .mapValues(JsonUtils::toJsonNode)
        .transformValues(() -> new EventSourcingTransformer(eventSourcingHandlers), "snapshots")
        .filter((key, snapshot) -> snapshot != null)
        .filter((key, snapshot) -> CommonUtils.getTopicInfo(snapshot) != null)
        .filter((key, snapshot) -> CommonUtils.getAggregateId(snapshot) != null);

    // Events --> Push
    events
        .transformValues(RevisionAdder::new)
        .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Snapshots --> Push
    snapshots
        .to((key, snapshot, recordContext) -> CommonUtils.getTopicInfo(snapshot).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Failed commands --> Push
    failedCommands
        .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command).value().concat(".errors"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Unprocessed commands --> Push
    unprocessed
        .to((key, command, recordContext) -> APPLICATION_ID.concat(".commands-retry"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));
  }

}

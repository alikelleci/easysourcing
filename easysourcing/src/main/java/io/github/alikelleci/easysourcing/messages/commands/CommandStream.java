package io.github.alikelleci.easysourcing.messages.commands;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.RevisionAdder;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Failure;
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
    // --> Commands
    KStream<String, JsonNode> commands = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);

    // Commands --> Command results
    KStream<String, CommandResult> commandResults = commands
        .transformValues(() -> new CommandTransformer(commandHandlers), "snapshot-store")
        .filter((key, result) -> result != null)
        .filter((key, result) -> result.getCommand() != null);

    // Successful results --> Events
    KStream<String, Object> events = commandResults
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result)
        .flatMapValues(Success::getEvents)
        .filter((key, event) -> event != null)
        .filter((key, event) -> CommonUtils.getTopicInfo(event) != null)
        .filter((key, event) -> CommonUtils.getAggregateId(event) != null);

    // Failed results --> Failed commands
    KStream<String, Object> failedCommands = commandResults
        .filter((key, result) -> result instanceof Failure)
        .mapValues((key, result) -> (Failure) result)
        .mapValues(CommandResult::getCommand)
        .filter((key, command) -> command != null)
        .filter((key, command) -> CommonUtils.getTopicInfo(command) != null)
        .filter((key, command) -> CommonUtils.getAggregateId(command) != null);

    // Events --> Snapshots
    KStream<String, Object> snapshots = events
        .mapValues(JsonUtils::toJsonNode)
        .transformValues(() -> new EventSourcingTransformer(eventSourcingHandlers), "snapshot-store")
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

  }

}

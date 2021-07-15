package io.github.alikelleci.easysourcing.messages.commands;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.messages.commands.transformers.AddEventHeaders;
import io.github.alikelleci.easysourcing.messages.commands.transformers.AddResultHeaders;
import io.github.alikelleci.easysourcing.messages.commands.transformers.AddSnapshotHeaders;
import io.github.alikelleci.easysourcing.messages.commands.transformers.FilterReplyTo;
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

import java.nio.charset.StandardCharsets;
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

    // Commands --> Results
    KStream<String, CommandResult> commandResults = commands
        .transformValues(() -> new CommandTransformer(commandHandlers), "snapshots")
        .filter((key, result) -> result != null);

    // Results --> Push
    commandResults
        .transformValues(AddResultHeaders::new)
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command).value().concat(".results"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Results --> Push reply
    commandResults
        .transformValues(FilterReplyTo::new)
        .filter((key, result) -> result != null)
        .transformValues(AddResultHeaders::new)
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> new String(recordContext.headers().lastHeader(Metadata.REPLY_TO).value(), StandardCharsets.UTF_8),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Success --> Events
    KStream<String, Object> events = commandResults
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result)
        .flatMapValues(Success::getEvents);

    // Events --> Push
    events
        .transformValues(AddEventHeaders::new)
        .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Events --> Snapshots
    KStream<String, Object> snapshots = events
        .mapValues(JsonUtils::toJsonNode)
        .transformValues(() -> new EventSourcingTransformer(eventSourcingHandlers), "snapshots");

    // Snapshots --> Push
    snapshots
        .transformValues(AddSnapshotHeaders::new)
        .to((key, snapshot, recordContext) -> CommonUtils.getTopicInfo(snapshot).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));
  }

}

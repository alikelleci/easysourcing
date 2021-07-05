package io.github.alikelleci.easysourcing.messages.commands;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.MessageTransformer;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Failure;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.messages.events.Event;
import io.github.alikelleci.easysourcing.messages.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.easysourcing.messages.eventsourcing.EventSourcingTransformer;
import io.github.alikelleci.easysourcing.messages.snapshots.Snapshot;
import io.github.alikelleci.easysourcing.messages.upcasters.PayloadTransformer;
import io.github.alikelleci.easysourcing.messages.upcasters.Upcaster;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
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
  private final MultiValuedMap<String, Upcaster> upcasters;
  private final Map<Class<?>, CommandHandler> commandHandlers;
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;

  public CommandStream(Set<String> topics, MultiValuedMap<String, Upcaster> upcasters, Map<Class<?>, CommandHandler> commandHandlers, Map<Class<?>, EventSourcingHandler> eventSourcingHandlers) {
    this.topics = topics;
    this.upcasters = upcasters;
    this.commandHandlers = commandHandlers;
    this.eventSourcingHandlers = eventSourcingHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Commands --> Command results
    KStream<String, CommandResult> commandResults = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, value) -> key != null)
        .filter((key, value) -> value != null)

        // Upcast & convert
        .transformValues(() -> new PayloadTransformer(upcasters))
        .transformValues(() -> new MessageTransformer<>(Command.class))

        // Filter
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getAggregateId() != null)

        // Invoke handlers
        .transformValues(() -> new CommandTransformer(commandHandlers), "snapshot-store");


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
        // Upcast & convert
        .transformValues(() -> new MessageTransformer<>(JsonNode.class))
        .transformValues(() -> new PayloadTransformer(upcasters))
        .transformValues(() -> new MessageTransformer<>(Event.class))

        // Filter
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getAggregateId() != null)

        // Invoke handlers
        .transformValues(() -> new EventSourcingTransformer(eventSourcingHandlers), "snapshot-store");


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

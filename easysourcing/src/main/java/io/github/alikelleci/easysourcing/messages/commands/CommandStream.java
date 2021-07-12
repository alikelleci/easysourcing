package io.github.alikelleci.easysourcing.messages.commands;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.RevisionAdder;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Error;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import io.github.alikelleci.easysourcing.util.CommonUtils;
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

  public CommandStream(Set<String> topics, Map<Class<?>, CommandHandler> commandHandlers) {
    this.topics = topics;
    this.commandHandlers = commandHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Commands
    KStream<String, JsonNode> commands = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);

    // Commands --> Command results
    KStream<String, Object> commandResults = commands
        .transformValues(() -> new CommandTransformer(commandHandlers), "redirects", "snapshots")
        .filter((key, result) -> result != null);

    // Successful results --> Events
    commandResults
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result)
        .flatMapValues(Success::getEvents)
        .transformValues(RevisionAdder::new)
        .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Error results --> Error commands
    commandResults
        .filter((key, result) -> result instanceof Error)
        .mapValues((key, result) -> (Error) result)
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command).value().concat(".errors"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Unprocessed --> Unprocessed commands
    commandResults
        .filter((key, result) -> !(result instanceof Success) && !(result instanceof Error))
        .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command).value().concat(".retry"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));
  }

}

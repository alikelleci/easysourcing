package io.github.alikelleci.easysourcing.messages.commands;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.RevisionAdder;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Successful;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Unprocessed;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
public class CommandStream {

  private final Set<String> topics;
  private final Map<Class<?>, CommandHandler> commandHandlers;

  public CommandStream(Set<String> topics, Map<Class<?>, CommandHandler> commandHandlers) {
    this.topics = topics;
    this.commandHandlers = commandHandlers;

    Set<String> copy = new HashSet<>(topics);
    topics.forEach(topic -> copy.add(topic + ".retry"));
    this.topics.addAll(copy);
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Commands
    KStream<String, JsonNode> commands = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);

    // Commands --> Results
    KStream<String, CommandResult> commandResults = commands
        .transformValues(() -> new CommandTransformer(commandHandlers), "redirects", "snapshots")
        .filter((key, result) -> result != null);

    // Successful --> Events
    KStream<String, Object> events = commandResults
        .filter((key, result) -> result instanceof Successful)
        .mapValues((key, result) -> (Successful) result)
        .flatMapValues(Successful::getEvents);

    // Results --> Push
    commandResults
        .filter((ke, commandResult) -> !(commandResult instanceof Unprocessed))
        .mapValues(CommandResult::getCommand)
        .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command).value().concat(".results"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Events --> Push
    events
        .transformValues(RevisionAdder::new)
        .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event).value(),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));

    // Unprocessed commands --> Push
    commandResults
        .filter((s, commandResult) -> commandResult instanceof Unprocessed)
        .mapValues((key, result) -> (Unprocessed) result)
        .mapValues((key, result) -> result.getCommand())
        .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event).value().concat(".retry"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));
  }

}

package io.github.alikelleci.easysourcing.messages.commands;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Failed;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Successful;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.validation.ValidationException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, JsonNode, CommandResult> {

  private final Map<Class<?>, CommandHandler> commandHandlers;
  private ProcessorContext context;
  private KeyValueStore<String, JsonNode> snapshots;

  public CommandTransformer(Map<Class<?>, CommandHandler> commandHandlers) {
    this.commandHandlers = commandHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.snapshots = context.getStateStore("snapshots");
  }

  @Override
  public CommandResult transform(String key, JsonNode jsonNode) {
    Object command = JsonUtils.toJavaType(jsonNode);
    if (command == null) {
      return null;
    }

    CommandHandler commandHandler = commandHandlers.get(command.getClass());
    if (commandHandler == null) {
      return null;
    }

    Object snapshot = Optional.ofNullable(snapshots.get(key))
        .map(JsonUtils::toJavaType)
        .orElse(null);

    Metadata metadata = Metadata.builder().build().injectContext(context);

    List<Object> events;
    try {
      events = commandHandler.invoke(command, snapshot, metadata);
    } catch (Exception e) {
      String message = ExceptionUtils.getRootCauseMessage(e);

      context.headers()
          .remove(Metadata.RESULT)
          .add(Metadata.RESULT, "failed".getBytes(StandardCharsets.UTF_8))
          .remove(Metadata.CAUSE)
          .add(Metadata.CAUSE, message.getBytes(StandardCharsets.UTF_8));

      if (ExceptionUtils.getRootCause(e) instanceof ValidationException) {
        log.debug("Command rejected: {}", message);
        return Failed.builder()
            .command(command)
            .message(message)
            .build();
      }
      throw e;
    }

    context.headers()
        .remove(Metadata.RESULT)
        .add(Metadata.RESULT, "successful".getBytes(StandardCharsets.UTF_8));

    return Successful.builder()
        .command(command)
        .events(events)
        .build();
  }

  @Override
  public void close() {

  }

}

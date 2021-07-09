package io.github.alikelleci.easysourcing.messages.commands;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Failure;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.validation.ValidationException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class CommandTransformer implements ValueTransformer<JsonNode, CommandResult> {

  private ProcessorContext context;
  private KeyValueStore<String, JsonNode> store;
  private final Map<Class<?>, CommandHandler> commandHandlers;

  public CommandTransformer(Map<Class<?>, CommandHandler> commandHandlers) {
    this.commandHandlers = commandHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.store = context.getStateStore("snapshot-store");
  }

  @Override
  public CommandResult transform(JsonNode jsonNode) {
    Object command = JsonUtils.toJavaType(jsonNode);
    if (command == null) {
      return null;
    }

    CommandHandler commandHandler = commandHandlers.get(command.getClass());
    if (commandHandler == null) {
      return null;
    }

    String key = CommonUtils.getAggregateId(command);

    Object snapshot = Optional.ofNullable(store.get(key))
        .map(JsonUtils::toJavaType)
        .orElse(null);

    Metadata metadata = Metadata.builder().build().injectContext(context);

    List<Object> events;
    try {
      events = commandHandler.invoke(command, snapshot, metadata);
    } catch (Exception e) {
      String message = ExceptionUtils.getRootCauseMessage(e);

      if (ExceptionUtils.getRootCause(e) instanceof ValidationException) {
        log.debug("Command rejected: {}", message);
        context.headers()
            .remove("$exception")
            .add("$exception", message.getBytes(StandardCharsets.UTF_8));

        return Failure.builder()
            .command(command)
            .message(message)
            .build();
      }
      throw e;
    }

    return Success.builder()
        .command(command)
        .events(events)
        .build();
  }

  @Override
  public void close() {

  }

}

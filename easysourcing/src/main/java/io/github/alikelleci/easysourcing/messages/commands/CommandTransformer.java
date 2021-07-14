package io.github.alikelleci.easysourcing.messages.commands;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Failure;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Unprocessed;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import javax.validation.ValidationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, JsonNode, CommandResult> {

  private final Map<Class<?>, CommandHandler> commandHandlers;
  private ProcessorContext context;
  private KeyValueStore<String, Long> timestamps;
  private KeyValueStore<String, ValueAndTimestamp<JsonNode>> snapshots;

  public CommandTransformer(Map<Class<?>, CommandHandler> commandHandlers) {
    this.commandHandlers = commandHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.timestamps = context.getStateStore("timestamps");
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

    ValueAndTimestamp<JsonNode> vt = snapshots.get(key);
    Long actualTimestamp = vt != null ? vt.timestamp() : -1L;
    Long expectedTimestamp = timestamps.get(key);
    if (expectedTimestamp != null && expectedTimestamp > actualTimestamp) {
      return Unprocessed.builder()
          .command(command)
          .build();
    }

    Object snapshot = Optional.ofNullable(vt)
        .map(ValueAndTimestamp::value)
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

        return Failure.builder()
            .command(command)
            .cause(message)
            .build();
      }
      throw e;
    }

    timestamps.put(key, context.timestamp());

    return Success.builder()
        .command(command)
        .events(events)
        .build();
  }

  @Override
  public void close() {

  }

}

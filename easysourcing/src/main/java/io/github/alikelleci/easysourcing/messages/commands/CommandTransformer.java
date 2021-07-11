package io.github.alikelleci.easysourcing.messages.commands;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.OperationMode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Error;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.validation.ValidationException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.alikelleci.easysourcing.EasySourcingBuilder.OPERATION_MODE;

@Slf4j
public class CommandTransformer implements Transformer<String, JsonNode, KeyValue<String, Object>> {

  private final Map<Class<?>, CommandHandler> commandHandlers;
  private ProcessorContext context;
  private KeyValueStore<String, Long> redirects;
  private KeyValueStore<String, JsonNode> snapshots;


  public CommandTransformer(Map<Class<?>, CommandHandler> commandHandlers) {
    this.commandHandlers = commandHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.redirects = context.getStateStore("command-redirects");
    this.snapshots = context.getStateStore("snapshots");
  }

  @Override
  public KeyValue<String, Object> transform(String key, JsonNode jsonNode) {
    Object command = JsonUtils.toJavaType(jsonNode);
    if (command == null) {
      return null;
    }

    CommandHandler commandHandler = commandHandlers.get(command.getClass());
    if (commandHandler == null) {
      return null;
    }

    if (redirects.get(key) != null) {
      if (OPERATION_MODE == OperationMode.NORMAL) {
        log.warn("Redirecting command {} ({})", command.getClass().getSimpleName(), key);
        return KeyValue.pair(key, command);
      }

      if (OPERATION_MODE == OperationMode.RETRY) {
        String error = Optional.ofNullable(context.headers().lastHeader("$error"))
            .map(Header::value)
            .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
            .orElse(null);

        if (StringUtils.isBlank(error)) {
          log.warn("Redirecting command {} ({})", command.getClass().getSimpleName(), key);
          return KeyValue.pair(key, command);
        }
      }
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
          .remove("$error")
          .add("$error", message.getBytes(StandardCharsets.UTF_8));

      if (ExceptionUtils.getRootCause(e) instanceof ValidationException) {
        log.debug("Command rejected: {}", message);

        redirects.put(key, null);
        return KeyValue.pair(key, Error.builder()
            .command(command)
            .message(message)
            .build());
      }

      log.error("Command not processed: {}", message);

      redirects.put(key, 1L);
      return KeyValue.pair(key, command);
    }

    redirects.put(key, null);
    return KeyValue.pair(key, Success.builder()
        .command(command)
        .events(events)
        .build());
  }

  @Override
  public void close() {

  }

}

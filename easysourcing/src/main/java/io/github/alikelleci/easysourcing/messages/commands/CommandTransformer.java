package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Failure;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import io.github.alikelleci.easysourcing.messages.events.Event;
import io.github.alikelleci.easysourcing.messages.snapshots.Snapshot;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.validation.ValidationException;
import java.util.List;
import java.util.Map;

import static io.github.alikelleci.easysourcing.messages.MetadataKeys.EXCEPTION;

@Slf4j
public class CommandTransformer implements ValueTransformer<Command, CommandResult> {

  private ProcessorContext context;
  private KeyValueStore<String, Snapshot> store;

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
  public CommandResult transform(Command command) {
    CommandHandler commandHandler = commandHandlers.get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    Snapshot snapshot = store.get(command.getAggregateId());
    List<Event> events;
    try {
      events = commandHandler.invoke(snapshot, command, context);
    } catch (Exception e) {
      if (ExceptionUtils.getRootCause(e) instanceof ValidationException) {
        String message = ExceptionUtils.getRootCauseMessage(e);

        log.debug("Command rejected: {}", message);
        return Failure.builder()
            .command(command.toBuilder()
                .metadata(command.getMetadata()
                    .add(EXCEPTION, message))
                .build())
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

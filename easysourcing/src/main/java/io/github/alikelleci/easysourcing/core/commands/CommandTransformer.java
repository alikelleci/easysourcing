package io.github.alikelleci.easysourcing.core.commands;

import io.github.alikelleci.easysourcing.common.MetadataKeys;
import io.github.alikelleci.easysourcing.core.aggregates.Aggregate;
import io.github.alikelleci.easysourcing.core.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.validation.ValidationException;
import java.util.List;
import java.util.Map;

import static io.github.alikelleci.easysourcing.common.MetadataKeys.EXCEPTION;

@Slf4j
public class CommandTransformer implements ValueTransformer<Command, CommandResult> {

  private ProcessorContext context;
  private KeyValueStore<String, Aggregate> store;

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

    Aggregate aggregate = store.get(command.getAggregateId());
    List<Event> events;
    try {
      events = commandHandler.invoke(aggregate, command, context);
    } catch (Exception e) {
      if (ExceptionUtils.getRootCause(e) instanceof ValidationException) {
        String message = ExceptionUtils.getRootCauseMessage(e);

        log.debug("Command rejected: {}", message);
        return CommandResult.Failure.builder()
            .command(command.toBuilder()
                .metadata(command.getMetadata().toBuilder()
                    .entry(EXCEPTION, message)
                    .build())
                .build())
            .message(message)
            .build();
      }
      throw e;
    }

    return CommandResult.Success.builder()
        .command(command)
        .events(events)
        .build();
  }

  @Override
  public void close() {

  }

}

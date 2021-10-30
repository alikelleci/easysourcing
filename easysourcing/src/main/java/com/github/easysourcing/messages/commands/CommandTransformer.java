package com.github.easysourcing.messages.commands;

import com.github.easysourcing.constants.Handlers;
import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.validation.ValidationException;
import java.util.List;
import java.util.Map;

@Slf4j
public class CommandTransformer implements ValueTransformer<Command, CommandResult> {

  private ProcessorContext context;
  private KeyValueStore<String, Aggregate> store;

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.store = context.getStateStore("snapshot-store");
  }

  @Override
  public CommandResult transform(Command command) {
    CommandHandler commandHandler = Handlers.COMMAND_HANDLERS.get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    Aggregate aggregate = store.get(command.getAggregateId());
    List<Event> events;
    try {
      events = commandHandler.invoke(aggregate, command, context);
    } catch (Exception e) {
      if (ExceptionUtils.getRootCause(e) instanceof ValidationException) {
        log.debug("Command rejected: {}", ExceptionUtils.getRootCauseMessage(e));
        return CommandResult.Failure.builder()
            .command(command)
            .message(ExceptionUtils.getRootCauseMessage(e))
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

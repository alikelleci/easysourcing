package com.github.easysourcing.messages.commands;

import com.github.easysourcing.constants.Handlers;
import com.github.easysourcing.messages.aggregates.Aggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

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

    // 1. Load aggregate
    Aggregate aggregate = store.get(command.getAggregateId());

    // 2. Validate command
    return commandHandler.invoke(aggregate, command, context);
  }

  @Override
  public void close() {

  }

}

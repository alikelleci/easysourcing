package com.github.easysourcing.messaging.commandhandling;

import com.github.easysourcing.EasySourcing;
import com.github.easysourcing.messaging.eventsourcing.Aggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, Command, CommandResult> {

  private final EasySourcing easySourcing;
  private ProcessorContext context;
  private KeyValueStore<String, Aggregate> snapshotStore;

  public CommandTransformer(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.snapshotStore = context.getStateStore("snapshot-store");
  }

  @Override
  public CommandResult transform(String key, Command command) {
    CommandHandler commandHandler = easySourcing.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    // 1. Load aggregate
    Aggregate aggregate = snapshotStore.get(command.getAggregateId());

    // 2. Validate command
    return commandHandler.apply(command, aggregate);
  }

  @Override
  public void close() {

  }

}

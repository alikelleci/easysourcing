package com.github.easysourcing.message.commands;

import com.github.easysourcing.message.aggregates.Aggregate;
import com.github.easysourcing.message.aggregates.AggregateService;
import com.github.easysourcing.message.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class CommandInvoker implements ValueTransformer<Command, List<Event>> {

  private ProcessorContext context;
  private KeyValueStore<String, ValueAndTimestamp<Aggregate>> store;
  private CommandService commandService;
  private AggregateService aggregateService;


  public CommandInvoker(CommandService commandService, AggregateService aggregateService) {
    this.commandService = commandService;
    this.aggregateService = aggregateService;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.store = (KeyValueStore<String, ValueAndTimestamp<Aggregate>>) context.getStateStore("snapshots");
  }

  @Override
  public List<Event> transform(Command command) {
    Method commandHandler = commandService.getCommandHandler(command);
    if (commandHandler == null) {
      return new ArrayList<>();
    }
    log.debug("Command received: {}", command);

    ValueAndTimestamp<Aggregate> record = store.get(command.getId());
    Aggregate aggregate = record != null ? record.value() : null;

    List<Event> events = commandService.invokeCommandHandler(commandHandler, aggregate, command);

    for (Event event : events) {
      Method aggregateHandler = aggregateService.getAggregateHandler(event);
      if (aggregateHandler != null) {
        aggregate = aggregateService.invokeAggregateHandler(aggregateHandler, aggregate, event);
      }
    }

    if (!events.isEmpty()) {
      store.put(command.getId(), ValueAndTimestamp
          .make(aggregate, new Timestamp(System.currentTimeMillis()).getTime()));
    }

    context.commit();
    return events;
  }

  @Override
  public void close() {

  }


}

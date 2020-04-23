package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;


@Slf4j
public class CommandTransformer implements ValueTransformer<Command, List<Event>> {

  private ProcessorContext context;
  private KeyValueStore<String, ValueAndTimestamp<Aggregate>> store;

  private final ConcurrentMap<Class<?>, CommandHandler> commandHandlers;
  private final ConcurrentMap<Class<?>, Aggregator> aggregators;

  public CommandTransformer(ConcurrentMap<Class<?>, CommandHandler> commandHandlers, ConcurrentMap<Class<?>, Aggregator> aggregators) {
    this.commandHandlers = commandHandlers;
    this.aggregators = aggregators;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.store = (KeyValueStore<String, ValueAndTimestamp<Aggregate>>) context.getStateStore("snapshot-store");
  }

  @Override
  public List<Event> transform(Command command) {
    CommandHandler commandHandler = commandHandlers.get(command.getPayload().getClass());
    if (commandHandler == null) {
      return new ArrayList<>();
    }

    ValueAndTimestamp<Aggregate> record = store.get(command.getId());
    Aggregate aggregate = record != null ? record.value() : null;

    log.debug("Handling command: {}", command);
    List<Event> events = commandHandler.invoke(aggregate, command);

    boolean updated = false;
    for (Event event : events) {
      Aggregator aggregator = aggregators.get(event.getPayload().getClass());
      if (aggregator != null) {
        log.debug("Applying event: {}", event);
        aggregate = aggregator.invoke(aggregate, event);
        updated = true;
      }
    }

    if (updated) {
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

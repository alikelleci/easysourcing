package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.events.Event;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;


public class CommandTransformer implements ValueTransformer<Command, List<Event>> {

  private ProcessorContext context;
  private KeyValueStore<String, ValueAndTimestamp<Aggregate>> store;

  private final ConcurrentMap<Class<?>, CommandHandler> commandHandlers;
  private final ConcurrentMap<Class<?>, Aggregator> aggregators;
  private final boolean frequentCommits;

  public CommandTransformer(ConcurrentMap<Class<?>, CommandHandler> commandHandlers, ConcurrentMap<Class<?>, Aggregator> aggregators, boolean frequentCommits) {
    this.commandHandlers = commandHandlers;
    this.aggregators = aggregators;
    this.frequentCommits = frequentCommits;
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

    List<Event> events = commandHandler.invoke(aggregate, command);

    boolean updated = false;
    for (Event event : events) {
      Aggregator aggregator = aggregators.get(event.getPayload().getClass());
      if (aggregator != null) {
        aggregate = aggregator.invoke(aggregate, event);
        updated = true;
      }
    }

    if (updated) {
      store.put(command.getId(), ValueAndTimestamp
          .make(aggregate, new Timestamp(System.currentTimeMillis()).getTime()));
    }

    if (frequentCommits) {
      context.commit();
    }
    return events;
  }

  @Override
  public void close() {

  }


}

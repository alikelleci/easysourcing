package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.commands.CommandResult.Failure;
import com.github.easysourcing.messages.commands.CommandResult.Success;
import com.github.easysourcing.messages.events.Event;
import com.github.easysourcing.messages.snapshots.Snapshot;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import javax.validation.ValidationException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

public class CommandTransformer implements ValueTransformer<Command, CommandResult> {

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
  public CommandResult transform(Command command) {
    CommandHandler commandHandler = commandHandlers.get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    Aggregate aggregate = loadAggregate(command.getAggregateId());
    List<Event> events;
    try {
      events = commandHandler.invoke(aggregate, command);
    } catch (Exception e) {
      if (ExceptionUtils.getRootCause(e) instanceof ValidationException) {
        String message = ExceptionUtils.getRootCauseMessage(e);
        return Failure.builder()
            .message(message)
            .command(command.toBuilder()
                .metadata(command.getMetadata().toBuilder()
                    .entry("$failure", message)
                    .build())
                .build())
            .build();
      }
      throw e;
    }

    boolean updated = false;
    for (Event event : events) {
      Aggregator aggregator = aggregators.get(event.getPayload().getClass());
      if (aggregator != null) {
        aggregate = aggregator.invoke(aggregate, event);
        updated = true;
      }
    }

    Snapshot snapshot = null;
    if (updated) {
      store.put(command.getAggregateId(), ValueAndTimestamp
          .make(aggregate, new Timestamp(System.currentTimeMillis()).getTime()));

      snapshot = Snapshot.builder()
          .payload(aggregate.getPayload())
          .metadata(aggregate.getMetadata())
          .build();
    }

    if (frequentCommits) {
      context.commit();
    }
    return Success.builder()
        .snapshot(snapshot)
        .events(events)
        .build();
  }

  @Override
  public void close() {

  }

  private Aggregate loadAggregate(String id) {
    ValueAndTimestamp<Aggregate> record = store.get(id);
    return Optional.ofNullable(record)
        .map(ValueAndTimestamp::value)
        .orElse(null);
  }

}

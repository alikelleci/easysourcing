package io.github.alikelleci.easysourcing.messaging.commandhandling;

import io.github.alikelleci.easysourcing.EasySourcing;
import io.github.alikelleci.easysourcing.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.EventSourcingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Optional;

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
    commandHandler.setContext(context);

    // 1. Load aggregate state
    Aggregate aggregate = loadAggregate(key);

    // 2. Validate command against aggregate
    CommandResult result = commandHandler.apply(command, aggregate);

    if (result instanceof CommandResult.Success) {
      // 3. Apply events
      for (Event event : ((CommandResult.Success) result).getEvents()) {
        aggregate = applyEvent(aggregate, event);
      }

      // 4. Save snapshot
      Optional.ofNullable(aggregate)
          .ifPresent(aggr -> {
            log.debug("Creating snapshot: {}", aggr);
            saveSnapshot(aggr);
          });

      if (aggregate == null) {
        deleteSnapshot(key);
      }
    }

    return result;
  }

  @Override
  public void close() {

  }

  protected Aggregate loadAggregate(String aggregateId) {
    log.debug("Loading aggregate state...");
    Aggregate aggregate = loadFromSnapshot(aggregateId);

    log.debug("Current aggregate state: {}", aggregate);
    return aggregate;
  }

  protected Aggregate loadFromSnapshot(String aggregateId) {
    return snapshotStore.get(aggregateId);
  }

  protected Aggregate applyEvent(Aggregate aggregate, Event event) {
    EventSourcingHandler eventSourcingHandler = easySourcing.getEventSourcingHandlers().get(event.getPayload().getClass());
    if (eventSourcingHandler != null) {
      aggregate = eventSourcingHandler.apply(event, aggregate);
    }
    return aggregate;
  }

  protected void saveSnapshot(Aggregate aggregate) {
    snapshotStore.put(aggregate.getAggregateId(), aggregate);
  }

  private void deleteSnapshot(String key) {
    snapshotStore.delete(key);
  }
}

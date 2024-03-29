package io.github.alikelleci.easysourcing.messaging.commandhandling;

import io.github.alikelleci.easysourcing.EasySourcing;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.easysourcing.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.EventSourcingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, Command, CommandResult> {

  private final EasySourcing easySourcing;
  private ProcessorContext context;
  private KeyValueStore<String, Aggregate> snapshotStore;

  public CommandTransformer(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.snapshotStore = this.context.getStateStore("snapshot-store");
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

    // 2. Execute command
    CommandResult result = executeCommand(commandHandler, aggregate, command);

    if (result instanceof Success) {
      // 3. Apply events
      for (Event event : ((Success) result).getEvents()) {
        aggregate = applyEvent(aggregate, event);
      }

      // 4. Save snapshot
      if (aggregate != null) {
        log.debug("Creating snapshot: {}", aggregate);
        saveSnapshot(aggregate);
      } else {
        deleteSnapshot(key);
      }
    }

    return result;
  }

  @Override
  public void close() {

  }

  protected CommandResult executeCommand(CommandHandler commandHandler, Aggregate aggregate, Command command) {
    try {
      List<Event> events = commandHandler.apply(aggregate, command);
      return Success.builder()
          .command(command)
          .events(events)
          .build();

    } catch (Exception e) {
      return Failure.builder()
          .command(command)
          .cause(ExceptionUtils.getRootCauseMessage(e))
          .build();
    }
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
      eventSourcingHandler.setContext(context);
      aggregate = eventSourcingHandler.apply(aggregate, event);
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

package io.github.alikelleci.easysourcing.messaging.commandhandling;

import io.github.alikelleci.easysourcing.EasySourcing;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.easysourcing.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.EventSourcingHandler;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
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
    try {
      // Load aggregate state
      Aggregate aggregate = loadAggregate(key);

      // Execute command
      List<Event> events = executeCommand(aggregate, command);

      // Return if no events
      if (CollectionUtils.isEmpty(events)) {
        return null;
      }

      // Apply events
      for (Event event : events) {
        aggregate = applyEvent(aggregate, event);
      }

      // Save snapshot
      if (aggregate != null) {
        log.debug("Creating snapshot: {}", aggregate);
        saveSnapshot(aggregate);
      } else {
        deleteSnapshot(key);
      }

      // Return success
      return Success.builder()
          .command(command)
          .events(events)
          .build();

    } catch (Exception e) {
      // Log failure
      logFailure(e);

      // Return failure
      return Failure.builder()
          .command(command)
          .cause(ExceptionUtils.getRootCauseMessage(e))
          .build();
    }
  }

  @Override
  public void close() {

  }

  protected List<Event> executeCommand(Aggregate aggregate, Command command) {
    CommandHandler commandHandler = easySourcing.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return new ArrayList<>();
    }

    commandHandler.setContext(context);
    return commandHandler.apply(aggregate, command);
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

  private void logFailure(Exception e) {
    Throwable throwable = ExceptionUtils.getRootCause(e);
    if (throwable instanceof ValidationException) {
      log.debug("Handling command failed: ", throwable);
    } else {
      log.error("Handling command failed: ", throwable);
    }
  }
}

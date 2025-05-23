package io.github.alikelleci.easysourcing.messaging.commandhandling;

import io.github.alikelleci.easysourcing.EasySourcing;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.easysourcing.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.AggregateState;
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
  private KeyValueStore<String, AggregateState> snapshotStore;

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
      AggregateState state = loadAggregate(key);

      // Execute command
      List<Event> events = executeCommand(state, command);

      // Return if no events
      if (CollectionUtils.isEmpty(events)) {
        return null;
      }

      // Apply events
      for (Event event : events) {
        state = applyEvent(state, event);
      }

      // Save snapshot
      if (state != null) {
        log.debug("Creating snapshot: {}", state);
        saveSnapshot(state);
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

  protected List<Event> executeCommand(AggregateState state, Command command) {
    CommandHandler commandHandler = easySourcing.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return new ArrayList<>();
    }

    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
    commandHandler.setContext(context);
    return commandHandler.apply(state, command);
  }

  protected AggregateState loadAggregate(String aggregateId) {
    log.debug("Loading aggregate state...");
    AggregateState state = loadFromSnapshot(aggregateId);

    log.debug("Current aggregate state: {}", state);
    return state;
  }

  protected AggregateState loadFromSnapshot(String aggregateId) {
    return snapshotStore.get(aggregateId);
  }

  protected AggregateState applyEvent(AggregateState state, Event event) {
    EventSourcingHandler eventSourcingHandler = easySourcing.getEventSourcingHandlers().get(event.getPayload().getClass());
    if (eventSourcingHandler != null) {
      log.debug("Applying event: {} ({})", event.getType(), event.getAggregateId());
      eventSourcingHandler.setContext(context);
      state = eventSourcingHandler.apply(state, event);
    }
    return state;
  }

  protected void saveSnapshot(AggregateState state) {
    snapshotStore.put(state.getAggregateId(), state);
  }

  private void deleteSnapshot(String key) {
    snapshotStore.delete(key);
  }

  private void logFailure(Exception e) {
    Throwable throwable = ExceptionUtils.getRootCause(e);
    String message = ExceptionUtils.getRootCauseMessage(e);

    if (throwable instanceof ValidationException) {
      log.debug("Handling command failed: {}", message, throwable);
    } else {
      log.error("Handling command failed: {}", message, throwable);
    }
  }
}

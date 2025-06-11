package io.github.alikelleci.easysourcing.core.messaging.commandhandling;

import io.github.alikelleci.easysourcing.core.EasySourcing;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.easysourcing.core.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.easysourcing.core.messaging.eventsourcing.EventSourcingHandler;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CommandProcessor implements FixedKeyProcessor<String, Command, CommandResult> {

  private final EasySourcing easySourcing;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private KeyValueStore<String, AggregateState> snapshotStore;

  public CommandProcessor(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, CommandResult> context) {
    this.context = context;
    this.snapshotStore = this.context.getStateStore("snapshot-store");
  }

  @Override
  public void process(FixedKeyRecord<String, Command> fixedKeyRecord) {
    String key = fixedKeyRecord.key();
    Command command = fixedKeyRecord.value();

    try {
      // Execute command
      List<Event> events = executeCommand(key, command);

      // Return if no events
      if (CollectionUtils.isEmpty(events)) {
        return;
      }

      // Return success
      // Forward success
      context.forward(fixedKeyRecord.withValue(Success.builder()
          .command(command)
          .events(events)
          .build()));

    } catch (Exception e) {
      // Log failure
      logFailure(e);

      // Forward failure
      context.forward(fixedKeyRecord.withValue(Failure.builder()
          .command(command)
          .cause(ExceptionUtils.getRootCauseMessage(e))
          .build()));
    }
  }

  @Override
  public void close() {

  }

  protected List<Event> executeCommand(String aggregateId, Command command) {
    CommandHandler commandHandler = easySourcing.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return new ArrayList<>();
    }

    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
    AggregateState state = loadAggregate(aggregateId);
    commandHandler.setContext(context);
    List<Event> events = commandHandler.apply(state, command);

    // Apply events
    for (Event event : events) {
      state = applyEvent(state, event);
    }

    // Save snapshot
    if (state != null) {
      log.debug("Creating snapshot: {}", state);
      saveSnapshot(state);
    } else {
      deleteSnapshot(aggregateId);
    }

    return events;
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

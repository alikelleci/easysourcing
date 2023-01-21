package io.github.alikelleci.easysourcing.messaging.commandhandling;

import io.github.alikelleci.easysourcing.messaging.eventhandling.Event;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

import static io.github.alikelleci.easysourcing.messaging.Metadata.FAILURE;
import static io.github.alikelleci.easysourcing.messaging.Metadata.RESULT;


public interface CommandResult {

  Command getCommand();

  @Value
  @Builder
  class Success implements CommandResult {
    Command command;
    @Singular
    List<Event> events;

    @Override
    public Command getCommand() {
      return command.toBuilder()
          .metadata(command.getMetadata().toBuilder()
              .entry(RESULT, "success")
              .build())
          .build();
    }
  }


  @Value
  @Builder
  class Failure implements CommandResult {
    Command command;
    String cause;

    @Override
    public Command getCommand() {
      return command.toBuilder()
          .metadata(command.getMetadata().toBuilder()
              .entry(RESULT, "failed")
              .entry(FAILURE, cause)
              .build())
          .build();
    }
  }
}

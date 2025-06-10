package io.github.alikelleci.easysourcing.core.messaging.commandhandling;

import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.eventhandling.Event;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;


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
      command.getMetadata()
          .add(Metadata.RESULT, "success")
          .remove(Metadata.FAILURE);

      return command;
    }
  }

  @Value
  @Builder
  class Failure implements CommandResult {
    Command command;
    String cause;

    @Override
    public Command getCommand() {
      command.getMetadata()
          .add(Metadata.RESULT, "failed")
          .add(Metadata.FAILURE, cause);

      return command;
    }
  }

}

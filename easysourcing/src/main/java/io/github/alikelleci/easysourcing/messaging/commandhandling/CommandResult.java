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
    private Command command;
    @Singular
    private List<Event> events;

    @Override
    public Command getCommand() {
      command.getMetadata()
          .add(RESULT, "success");

      return command;
    }
  }

  @Value
  @Builder
  class Failure implements CommandResult {
    private Command command;
    private String cause;

    @Override
    public Command getCommand() {
      command.getMetadata()
          .add(RESULT, "failed")
          .add(FAILURE, cause);

      return command;
    }
  }

}

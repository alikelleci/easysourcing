package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.events.Event;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

import static com.github.easysourcing.messages.Metadata.FAILURE;
import static com.github.easysourcing.messages.Metadata.RESULT;


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
    String message;

    @Override
    public Command getCommand() {
      return command.toBuilder()
          .metadata(command.getMetadata().toBuilder()
              .entry(RESULT, "failed")
              .entry(FAILURE, message)
              .build())
          .build();
    }
  }
}

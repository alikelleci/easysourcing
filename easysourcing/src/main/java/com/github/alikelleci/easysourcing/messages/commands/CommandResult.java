package com.github.alikelleci.easysourcing.messages.commands;

import com.github.alikelleci.easysourcing.messages.MetadataKeys;
import com.github.alikelleci.easysourcing.messages.events.Event;
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
      return command.toBuilder()
          .metadata(command.getMetadata().toBuilder()
              .entry(MetadataKeys.RESULT, "success")
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
              .entry(MetadataKeys.RESULT, "failed")
              .entry(MetadataKeys.FAILURE, message)
              .build())
          .build();
    }
  }
}

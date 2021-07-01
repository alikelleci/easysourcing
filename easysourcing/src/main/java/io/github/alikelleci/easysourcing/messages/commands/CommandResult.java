package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.messages.events.Event;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;


public interface CommandResult {

  Command getCommand();

  @Value
  @Builder
  class Success implements CommandResult {
    private Command command;
    @Singular
    private List<Event> events;
  }


  @Value
  @Builder
  class Failure implements CommandResult {
    private Command command;
    private String message;
  }

}

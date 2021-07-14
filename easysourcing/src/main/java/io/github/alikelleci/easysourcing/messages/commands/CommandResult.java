package io.github.alikelleci.easysourcing.messages.commands;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;


public interface CommandResult {

  Object getCommand();

  @Value
  @Builder
  class Success implements CommandResult {
    private Object command;
    @Singular
    private List<Object> events;
  }

  @Value
  @Builder
  class Failure implements CommandResult {
    private Object command;
    private String message;
  }

  @Value
  @Builder
  class Unprocessed implements CommandResult {
    private Object command;
  }

}

package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.events.Event;
import com.github.easysourcing.messages.snapshots.Snapshot;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

public interface CommandResult {

  @Value
  @Builder
  class Success implements CommandResult {
    private Snapshot snapshot;
    @Singular
    private List<Event> events;
  }


  @Value
  @Builder
  class Failure implements CommandResult {
    private String message;
    private Command command;
  }
}

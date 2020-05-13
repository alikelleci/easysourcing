package com.github.easysourcing.messages.commands.results;

import com.github.easysourcing.messages.events.Event;
import com.github.easysourcing.messages.snapshots.Snapshot;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class Success implements CommandResult {
  private Snapshot snapshot;
  @Singular
  private List<Event> events;
}

package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.events.Event;
import com.github.easysourcing.utils.MetadataUtils;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.commons.collections4.ListUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public interface CommandResult {

  Command getCommand();

  @Value
  @Builder
  class Success implements CommandResult {
    private Command command;
    private Aggregate snapshot;
    @Singular
    private List<Event> events;

    @Override
    public Command getCommand() {
      return command.toBuilder()
          .metadata(MetadataUtils.filterMetadata(command.getMetadata()).toBuilder()
              .entry("$id", command.getMetadata().getEntries().get("$id"))
              .entry("$result", "success")
              .entry("$snapshot", Optional.ofNullable(snapshot)
                  .map(s -> s.getMetadata().getEntries().get("$id"))
                  .orElse(""))
              .entry("$events", ListUtils.emptyIfNull(events).stream()
                  .map(event -> event.getMetadata().getEntries().get("$id"))
                  .collect(Collectors.joining(",")))
              .build())
          .build();
    }
  }


  @Value
  @Builder
  class Failure implements CommandResult {
    private Command command;
    private String message;

    @Override
    public Command getCommand() {
      return command.toBuilder()
          .metadata(MetadataUtils.filterMetadata(command.getMetadata()).toBuilder()
              .entry("$id", command.getMetadata().getEntries().get("$id"))
              .entry("$result", "failed")
              .entry("$failure", message)
              .build())
          .build();
    }
  }
}

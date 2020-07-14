package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.events.Event;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.commons.collections4.ListUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.easysourcing.messages.MetadataKeys.EVENTS;
import static com.github.easysourcing.messages.MetadataKeys.FAILURE;
import static com.github.easysourcing.messages.MetadataKeys.ID;
import static com.github.easysourcing.messages.MetadataKeys.RESULT;
import static com.github.easysourcing.messages.MetadataKeys.SNAPSHOT;


public interface CommandResult {

  Command getCommand();

  @Value
  @Builder
  class Success implements CommandResult {
    Command command;
    Aggregate snapshot;
    @Singular
    List<Event> events;

    @Override
    public Command getCommand() {
      return command.toBuilder()
          .metadata(command.getMetadata().toBuilder()
              .entry(RESULT, "success")
              .entry(SNAPSHOT, Optional.ofNullable(snapshot)
                  .map(s -> s.getMetadata().get(ID))
                  .orElse(""))
              .entry(EVENTS, ListUtils.emptyIfNull(events).stream()
                  .map(event -> event.getMetadata().get(ID))
                  .collect(Collectors.joining(",")))
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

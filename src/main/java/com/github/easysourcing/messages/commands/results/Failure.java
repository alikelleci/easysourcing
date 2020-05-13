package com.github.easysourcing.messages.commands.results;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Failure implements CommandResult {
  private String message;
}

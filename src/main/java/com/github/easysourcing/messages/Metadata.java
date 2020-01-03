package com.github.easysourcing.messages;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.Map;

@Value
@Builder(toBuilder = true)
public class Metadata {

  @Singular
  private Map<String, String> entries;
}

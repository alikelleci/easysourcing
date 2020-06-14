package com.github.easysourcing.messages;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.beans.Transient;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder(toBuilder = true)
public class Metadata {

  @Singular
  private Map<String, String> entries;


  @Transient
  public Metadata filter() {
    Map<String, String> map = new HashMap<>(entries);
    map.keySet().removeIf(key ->
        StringUtils.equalsAny(key, "$id", "$result", "$snapshot", "$events", "$failure", "$timestamp"));

    return this.toBuilder()
        .clearEntries()
        .entries(map)
        .build();
  }

  @Transient
  public Metadata inject(ProcessorContext context) {
    return this.toBuilder()
        .entry("$timestamp", String.valueOf(context.timestamp()))
        .build();
  }
}

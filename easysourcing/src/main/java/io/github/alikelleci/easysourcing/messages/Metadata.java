package io.github.alikelleci.easysourcing.messages;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.beans.Transient;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder(toBuilder = true)
public class Metadata {
  @Singular
  private Map<String, String> entries;

  private long timestamp;
  private String topic;
  private int partition;
  private long offset;


  @Transient
  public Metadata filter() {
    Map<String, String> map = new HashMap<>(entries);
    map.keySet().removeIf(key -> StringUtils.startsWithIgnoreCase(key, "$"));

    return this.toBuilder()
        .clearEntries()
        .entries(map)
        .build();
  }

  @Transient
  public Metadata injectContext(ProcessorContext context) {
    Map<String, String> map = new HashMap<>();
    context.headers().forEach(header ->
        map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8)));

    return this.toBuilder()
        .entries(map)
        .timestamp(context.timestamp())
        .topic(context.topic())
        .partition(context.partition())
        .offset(context.offset())
        .build();
  }

  public String get(String key) {
    return entries.get(key);
  }

  public Metadata add(String key, String value) {
    return this.toBuilder()
        .entry(key, value)
        .build();
  }

}

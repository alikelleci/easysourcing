package io.github.alikelleci.easysourcing.messages;

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
  private Map<String, Object> entries;

  private long timestamp;
  private String topic;
  private int partition;
  private long offset;


  @Transient
  public Metadata filter() {
    Map<String, Object> map = new HashMap<>(entries);
    map.keySet().removeIf(key -> StringUtils.startsWithIgnoreCase(key, "$"));

    return this.toBuilder()
        .clearEntries()
        .entries(map)
        .build();
  }

  @Transient
  public Metadata injectContext(ProcessorContext context) {
    return this.toBuilder()
        .timestamp(context.timestamp())
        .topic(context.topic())
        .partition(context.partition())
        .offset(context.offset())
        .build();
  }

  public Object get(String key) {
    return entries.get(key);
  }

}

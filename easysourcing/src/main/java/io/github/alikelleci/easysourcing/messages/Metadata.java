package io.github.alikelleci.easysourcing.messages;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.beans.Transient;
import java.util.HashMap;
import java.util.Map;

import static io.github.alikelleci.easysourcing.messages.MetadataKeys.OFFSET;
import static io.github.alikelleci.easysourcing.messages.MetadataKeys.PARTITION;
import static io.github.alikelleci.easysourcing.messages.MetadataKeys.TIMESTAMP;
import static io.github.alikelleci.easysourcing.messages.MetadataKeys.TOPIC;


@Value
@Builder(toBuilder = true)
public class Metadata {

  @Singular
  Map<String, String> entries;

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
  public Metadata inject(ProcessorContext context) {
    return this.toBuilder()
        .entry(TIMESTAMP, String.valueOf(context.timestamp()))
        .entry(TOPIC, String.valueOf(context.topic()))
        .entry(PARTITION, String.valueOf(context.partition()))
        .entry(OFFSET, String.valueOf(context.offset()))
        .build();
  }

  @Transient
  public String get(String key) {
    return entries.get(key);
  }
}

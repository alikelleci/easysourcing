package com.github.easysourcing.messaging;

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
  public static final String ID = "$id";
  public static final String CORRELATION_ID = "$correlationId";
  public static final String RESULT = "$result";
  public static final String FAILURE = "$failure";

  public static final String TIMESTAMP = "$timestamp";
  public static final String TOPIC = "$topic";
  public static final String PARTITION = "$partition";
  public static final String OFFSET = "$offset";

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

package io.github.alikelleci.easysourcing.messages;

import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.beans.Transient;
import java.util.HashMap;
import java.util.Map;

@ToString
public class Metadata {
  private Map<String, Object> values = new HashMap<>();
  private long timestamp;
  private String topic;
  private int partition;
  private long offset;

  public Metadata() {
  }

  public Metadata injectContext(ProcessorContext context) {
    timestamp = context.timestamp();
    topic = context.topic();
    partition = context.partition();
    offset = context.offset();

    return this;
  }

  @Transient
  public Metadata filter() {
    values.keySet().removeIf(key ->
        StringUtils.startsWithIgnoreCase(key, "$"));

    return this;
  }

  public Metadata add(String key, Object value) {
    values.put(key, value);
    return this;
  }

  public Metadata remove(String key) {
    values.remove(key);
    return this;
  }

  public Object get(String key) {
    return values.get(key);
  }

  public Map<String, Object> getValues() {
    return values;
  }

  @Transient
  public long getTimestamp() {
    return timestamp;
  }

  @Transient
  public String getTopic() {
    return topic;
  }

  @Transient
  public int getPartition() {
    return partition;
  }

  @Transient
  public long getOffset() {
    return offset;
  }
}

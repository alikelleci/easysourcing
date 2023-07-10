package io.github.alikelleci.easysourcing.messaging;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.beans.Transient;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Getter
@ToString
@EqualsAndHashCode
public class Metadata {
  public static final String ID = "$id";
  public static final String TIMESTAMP = "$timestamp";
  public static final String CORRELATION_ID = "$correlationId";
  public static final String REPLY_TO = "$replyTo";
  public static final String RESULT = "$result";
  public static final String FAILURE = "$failure";
  public static final String EVENT_ID = "$eventId";

  private Map<String, String> entries;

  protected Metadata() {
    this.entries = new HashMap<>();
  }

  protected Metadata(Map<String, String> entries) {
    this.entries = entries;
  }

  public Metadata addAll(Metadata metadata) {
    if (metadata != null) {
      this.entries.putAll(new HashMap<>(metadata.getEntries()));
    }
    return this;
  }

  public Metadata add(String key, String value) {
    this.entries.put(key, value);
    return this;
  }

  public Metadata remove(String key) {
    this.entries.remove(key);
    return this;
  }

  @Transient
  public Metadata inject(ProcessorContext context) {
    entries.put(TIMESTAMP, String.valueOf(context.timestamp()));
    return this;
  }

  @Transient
  public String get(String key) {
    return entries.get(key);
  }

  @Transient
  public String getMessageId() {
    return this.entries.get(ID);
  }

  @Transient
  public Instant getTimestamp() {
    return Optional.ofNullable(this.entries.get(TIMESTAMP))
        .map(Long::parseLong)
        .map(Instant::ofEpochMilli)
        .orElse(null);
  }

  public static MetadataBuilder builder() {
    return new MetadataBuilder();
  }

  public static class MetadataBuilder {

    private Map<String, String> entries = new HashMap<>();

    public MetadataBuilder addAll(Metadata metadata) {
      if (metadata != null) {
        this.entries.putAll(new HashMap<>(metadata.getEntries()));
      }
      return this;
    }

    public MetadataBuilder add(String key, String value) {
      this.entries.put(key, value);
      return this;
    }

    public MetadataBuilder remove(String key) {
      this.entries.remove(key);
      return this;
    }

    public Metadata build() {
      return new Metadata(this.entries);
    }
  }

}

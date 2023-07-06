package io.github.alikelleci.easysourcing.messaging;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Delegate;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.beans.Transient;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ToString
public class Metadata implements Map<String, String> {
  public static final String ID = "$id";
  public static final String TIMESTAMP = "$timestamp";
  public static final String CORRELATION_ID = "$correlationId";
  public static final String REPLY_TO = "$replyTo";
  public static final String REVISION = "$revision";
  public static final String RESULT = "$result";
  public static final String CAUSE = "$cause";

  @Delegate
  private Map<String, String> entries;

  protected Metadata() {
    this.entries = new HashMap<>();
  }

  protected Metadata(Map<String, String> entries) {
    this.entries = entries;
  }

  public Metadata addAll(Metadata metadata) {
    if (metadata != null) {
      this.entries.putAll(new HashMap<>(metadata));
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
  protected Metadata filter() {
    entries.keySet().removeIf(key -> StringUtils.startsWithIgnoreCase(key, "$"));
    return this;
  }

  @Transient
  public String getMessageId() {
    return this.entries.get(ID);
  }

  @Transient
  public Instant getTimestamp() {
    return Instant.parse(this.entries.get(TIMESTAMP));
  }

  public static MetadataBuilder builder() {
    return new MetadataBuilder();
  }

  public static class MetadataBuilder {

    private Map<String, String> entries = new HashMap<>();

    public MetadataBuilder addAll(Metadata metadata) {
      if (metadata != null) {
        this.entries.putAll(new HashMap<>(metadata));
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

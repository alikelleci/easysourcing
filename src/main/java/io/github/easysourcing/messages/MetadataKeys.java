package io.github.easysourcing.messages;

public final class MetadataKeys {
  public static final String ID = "$id";
  public static final String CORRELATION_ID = "$correlationId";
  public static final String RESULT = "$result";
  public static final String FAILURE = "$failure";

  public static final String TIMESTAMP = "$timestamp";
  public static final String TOPIC = "$topic";
  public static final String PARTITION = "$partition";
  public static final String OFFSET = "$offset";

  private MetadataKeys() {
  }
}

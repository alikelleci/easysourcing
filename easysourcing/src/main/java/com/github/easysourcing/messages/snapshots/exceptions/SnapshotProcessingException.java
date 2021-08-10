package com.github.easysourcing.messages.snapshots.exceptions;

public class SnapshotProcessingException extends RuntimeException {

  public SnapshotProcessingException(String message) {
    super(message);
  }

  public SnapshotProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}

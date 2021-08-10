package com.github.easysourcing.messages.exceptions;

public class AggregateIdMissingException extends RuntimeException {

  public AggregateIdMissingException(String message) {
    super(message);
  }

  public AggregateIdMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}

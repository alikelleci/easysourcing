package com.github.easysourcing.common.exceptions;

public class AggregateIdMissingException extends RuntimeException {

  public AggregateIdMissingException(String message) {
    super(message);
  }

  public AggregateIdMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}

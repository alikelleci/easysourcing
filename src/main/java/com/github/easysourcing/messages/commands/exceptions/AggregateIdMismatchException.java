package com.github.easysourcing.messages.commands.exceptions;

public class AggregateIdMismatchException extends RuntimeException {

  public AggregateIdMismatchException(String message) {
    super(message);
  }

  public AggregateIdMismatchException(String message, Throwable cause) {
    super(message, cause);
  }
}

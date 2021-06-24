package io.github.alikelleci.easysourcing.messages.exceptions;

public class AggregateIdMismatchException extends RuntimeException {

  public AggregateIdMismatchException(String message) {
    super(message);
  }

  public AggregateIdMismatchException(String message, Throwable cause) {
    super(message, cause);
  }
}

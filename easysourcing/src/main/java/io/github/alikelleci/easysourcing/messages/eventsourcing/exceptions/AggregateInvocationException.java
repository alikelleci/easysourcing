package io.github.alikelleci.easysourcing.messages.eventsourcing.exceptions;

public class AggregateInvocationException extends RuntimeException {

  public AggregateInvocationException(String message) {
    super(message);
  }

  public AggregateInvocationException(String message, Throwable cause) {
    super(message, cause);
  }
}

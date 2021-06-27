package io.github.alikelleci.easysourcing.messages.aggregates.exceptions;

public class AggregateInvocationException extends RuntimeException {

  public AggregateInvocationException(String message) {
    super(message);
  }

  public AggregateInvocationException(String message, Throwable cause) {
    super(message, cause);
  }
}

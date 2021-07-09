package io.github.alikelleci.easysourcing.messages.errors.exceptions;

public class ErrorProcessingException extends RuntimeException {

  public ErrorProcessingException(String message) {
    super(message);
  }

  public ErrorProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}

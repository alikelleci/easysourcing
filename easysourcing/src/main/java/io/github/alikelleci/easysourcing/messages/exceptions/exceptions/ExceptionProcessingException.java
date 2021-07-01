package io.github.alikelleci.easysourcing.messages.exceptions.exceptions;

public class ExceptionProcessingException extends RuntimeException {

  public ExceptionProcessingException(String message) {
    super(message);
  }

  public ExceptionProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}

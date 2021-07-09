package io.github.alikelleci.easysourcing.messages.upcasters.exceptions;

public class UpcastingException extends RuntimeException {

  public UpcastingException(String message) {
    super(message);
  }

  public UpcastingException(String message, Throwable cause) {
    super(message, cause);
  }
}

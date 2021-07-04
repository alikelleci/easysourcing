package io.github.alikelleci.easysourcing.messages.upcasters.exceptions;

public class UpcastException extends RuntimeException {

  public UpcastException(String message) {
    super(message);
  }

  public UpcastException(String message, Throwable cause) {
    super(message, cause);
  }
}

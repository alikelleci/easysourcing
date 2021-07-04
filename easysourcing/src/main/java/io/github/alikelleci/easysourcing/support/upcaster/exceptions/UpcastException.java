package io.github.alikelleci.easysourcing.support.upcaster.exceptions;

public class UpcastException extends RuntimeException {

  public UpcastException(String message) {
    super(message);
  }

  public UpcastException(String message, Throwable cause) {
    super(message, cause);
  }
}

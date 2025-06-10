package io.github.alikelleci.easysourcing.core.common.exceptions;

public class PayloadMissingException extends RuntimeException {

  public PayloadMissingException(String message) {
    super(message);
  }

  public PayloadMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}

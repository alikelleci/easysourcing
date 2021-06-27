package com.github.alikelleci.easysourcing.messages.events.exceptions;

public class EventProcessingException extends RuntimeException {

  public EventProcessingException(String message) {
    super(message);
  }

  public EventProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}

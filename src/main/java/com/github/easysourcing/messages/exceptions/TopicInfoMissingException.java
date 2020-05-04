package com.github.easysourcing.messages.exceptions;

public class TopicInfoMissingException extends RuntimeException {

  public TopicInfoMissingException(String message) {
    super(message);
  }

  public TopicInfoMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}

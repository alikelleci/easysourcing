package com.github.easysourcing.messages.commands.exceptions;

public class InvalidCommandException extends RuntimeException {

  public InvalidCommandException(String message) {
    super(message);
  }

  public InvalidCommandException(String message, Throwable cause) {
    super(message, cause);
  }
}

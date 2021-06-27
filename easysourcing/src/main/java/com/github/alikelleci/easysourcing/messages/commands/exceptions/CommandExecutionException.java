package com.github.alikelleci.easysourcing.messages.commands.exceptions;

public class CommandExecutionException extends RuntimeException {

  public CommandExecutionException(String message) {
    super(message);
  }

  public CommandExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}

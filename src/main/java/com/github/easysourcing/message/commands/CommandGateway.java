package com.github.easysourcing.message.commands;

import com.github.easysourcing.message.MessageGateway;
import com.github.easysourcing.message.Metadata;

public class CommandGateway {

  private final MessageGateway messageGateway;

  public CommandGateway(MessageGateway messageGateway) {
    this.messageGateway = messageGateway;
  }

  public void send(Object payload, Metadata metadata) {
    Command message = Command.builder()
        .payload(payload)
        .metadata(metadata)
        .build();

    messageGateway.send(message);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}

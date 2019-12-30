package com.github.easysourcing.message.events;

import com.github.easysourcing.message.MessageGateway;
import com.github.easysourcing.message.Metadata;

public class EventGateway {

  private final MessageGateway messageGateway;

  public EventGateway(MessageGateway messageGateway) {
    this.messageGateway = messageGateway;
  }

  public void send(Object payload, Metadata metadata) {
    Event message = Event.builder()
        .payload(payload)
        .metadata(metadata)
        .build();

    messageGateway.send(message);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}

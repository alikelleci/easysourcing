package com.github.easysourcing.messages.events;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.Metadata;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

public class EventGateway extends MessageGateway {

  public EventGateway(KafkaTemplate<String, Message> kafkaTemplate) {
    super(kafkaTemplate);
  }

  public void send(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    Event message = Event.builder()
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry("$id", UUID.randomUUID().toString())
            .entry("$correlationId", UUID.randomUUID().toString())
            .build())
        .build();

    send(message);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}

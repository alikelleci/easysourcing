package com.github.easysourcing.messages.events;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

@Slf4j
public class EventGateway extends MessageGateway {

  public EventGateway(KafkaTemplate<String, Message> kafkaTemplate) {
    super(kafkaTemplate);
  }

  public void send(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    Event event = Event.builder()
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry("$id", UUID.randomUUID().toString())
            .entry("$correlationId", UUID.randomUUID().toString())
            .build())
        .build();

    log.info("Sending event: {}", event);
    send(event);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}

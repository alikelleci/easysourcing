package com.github.easysourcing.message.events;

import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.MessageGateway;
import com.github.easysourcing.message.Metadata;
import org.springframework.kafka.core.KafkaTemplate;

public class EventGateway extends MessageGateway {

  public EventGateway(KafkaTemplate<String, Message> kafkaTemplate) {
    super(kafkaTemplate);
  }

  public void send(Object payload, Metadata metadata) {
    Event message = Event.builder()
        .payload(payload)
        .metadata(metadata)
        .build();

    send(message);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}

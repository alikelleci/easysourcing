package com.github.easysourcing.message.commands;

import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.MessageGateway;
import com.github.easysourcing.message.Metadata;
import org.springframework.kafka.core.KafkaTemplate;

public class CommandGateway extends MessageGateway {

  public CommandGateway(KafkaTemplate<String, Message> kafkaTemplate) {
    super(kafkaTemplate);
  }

  public void send(Object payload, Metadata metadata) {
    Command message = Command.builder()
        .payload(payload)
        .metadata(metadata)
        .build();

    send(message);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}

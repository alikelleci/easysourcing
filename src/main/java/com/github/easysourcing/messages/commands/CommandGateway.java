package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.Metadata;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

public class CommandGateway extends MessageGateway {

  public CommandGateway(KafkaTemplate<String, Message> kafkaTemplate) {
    super(kafkaTemplate);
  }

  public void send(Object payload, Metadata metadata) {
    Command message = Command.builder()
        .uuid(UUID.randomUUID().toString())
        .payload(payload)
        .metadata(metadata)
        .build();

    send(message);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}

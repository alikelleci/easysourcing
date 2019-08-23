package org.easysourcing.api.message.commands;

import org.easysourcing.api.message.Message;
import org.easysourcing.api.message.MessageType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class CommandGateway {

  @Autowired
  private KafkaTemplate<String, Message> kafkaTemplate;

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;

  public <T> void send(T payload) {
    String topic = "events." + APPLICATION_ID;

    Message<T> message = Message.<T>builder()
        .type(MessageType.Command)
        .payload(payload)
        .build();

    kafkaTemplate.send(topic, message.getAggregateId(), message);
  }


}

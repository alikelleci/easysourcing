package com.github.easysourcing.message.commands;

import com.example.easysourcing.message.Message;
import com.example.easysourcing.message.MessageType;
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
    String topic = APPLICATION_ID.concat("-events");

    Message<T> message = Message.<T>builder()
        .type(MessageType.Command)
        .name(payload.getClass().getSimpleName())
        .payload(payload)
        .build();

    String aggregateId = message.getAggregateId();
    if (aggregateId == null) {
      throw new IllegalArgumentException("You are trying to dispatch a command without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
    }
    kafkaTemplate.send(topic, aggregateId, message);
  }


}

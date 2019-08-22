package com.example.easysourcing.message.commands;

import com.example.easysourcing.message.Message;
import com.example.easysourcing.message.MessageType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class CommandGateway {

  @Autowired
  private KafkaTemplate<String, Message> kafkaTemplate;


  public <T> void send(T payload) {
    String topic = payload.getClass().getPackage().getName().concat("-eventstore");

    Message<T> message = Message.<T>builder()
        .type(MessageType.Command)
        .payload(payload)
        .build();

    kafkaTemplate.send(topic, message.getAggregateId(), message);
  }



}

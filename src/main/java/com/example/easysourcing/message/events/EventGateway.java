package com.example.easysourcing.message.events;

import com.example.easysourcing.message.Message;
import com.example.easysourcing.message.MessageType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class EventGateway {

  @Autowired
  private KafkaTemplate<String, Message> kafkaTemplate;


  public <T> void send(T payload) {
    String topic = payload.getClass().getPackage().getName().concat("-eventstore");

    Message<T> message = Message.<T>builder()
        .type(MessageType.Event)
        .payload(payload)
        .build();

    kafkaTemplate.send(topic, message.getAggregateId(), message);
  }


}

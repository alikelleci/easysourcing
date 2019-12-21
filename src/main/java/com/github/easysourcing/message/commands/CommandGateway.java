package com.github.easysourcing.message.commands;

import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.Metadata;
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

  private <T> void send(String topic, T payload, Metadata metadata) {
    Command<T> message = Command.<T>builder()
        .type(payload.getClass().getSimpleName())
        .payload(payload)
        .metadata(metadata)
        .build();

    String aggregateId = message.getAggregateId();
    if (aggregateId == null) {
      throw new IllegalArgumentException("You are trying to dispatch a command without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
    }
    kafkaTemplate.send(topic, aggregateId, message);
  }

  public <T> void send(T payload, Metadata metadata) {
    String topic = APPLICATION_ID.concat("-events");
    this.send(topic, payload, metadata);
  }

  public <T> void send(T payload) {
    this.send(payload, null);
  }

  public <T> void sendTo(String applicationId, T payload, Metadata metadata) {
    String topic = applicationId.concat("-events");
    this.send(topic, payload, metadata);
  }

  public <T> void sendTo(String applicationId, T payload) {
    this.sendTo(applicationId, payload, null);
  }
}

package com.github.easysourcing.message.commands;

import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.Metadata;
import com.github.easysourcing.message.annotations.TopicInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class CommandGateway {

  @Value("${easysourcing.application-id}")
  private String APPLICATION_ID;

  @Autowired
  private KafkaTemplate<String, Message> kafkaTemplate;


  public <T> void send(T payload, Metadata metadata) {
    if (payload == null) {
      throw new IllegalArgumentException("You are trying to dispatch a command without a payload.");
    }

    Command<T> message = Command.<T>builder()
        .payload(payload)
        .metadata(metadata)
        .build();

    TopicInfo topicInfo = AnnotationUtils.findAnnotation(payload.getClass(), TopicInfo.class);
    if (topicInfo == null) {
      throw new IllegalArgumentException("You are trying to dispatch a command without any topic information. Please annotate your command with @TopicInfo.");
    }

    String aggregateId = message.getId();
    if (aggregateId == null) {
      throw new IllegalArgumentException("You are trying to dispatch a command without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
    }

    kafkaTemplate.send(topicInfo.value(), aggregateId, message);
  }

  public <T> void send(T payload) {
    this.send(payload, null);
  }

}

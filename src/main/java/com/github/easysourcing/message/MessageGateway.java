package com.github.easysourcing.message;

import com.github.easysourcing.message.annotations.TopicInfo;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.core.KafkaTemplate;

public class MessageGateway {

  private final KafkaTemplate<String, Message> kafkaTemplate;

  public MessageGateway(KafkaTemplate<String, Message> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void send(Message message) {
    if (message.getPayload() == null) {
      throw new IllegalArgumentException("You are trying to dispatch a message without a payload.");
    }

    TopicInfo topicInfo = AnnotationUtils.findAnnotation(message.getPayload().getClass(), TopicInfo.class);
    if (topicInfo == null) {
      throw new IllegalArgumentException("You are trying to dispatch a message without any topic information. Please annotate your message with @TopicInfo.");
    }

    String aggregateId = message.getId();
    if (aggregateId == null) {
      throw new IllegalArgumentException("You are trying to dispatch a message without a proper identifier. Please annotate your field containing the identifier with @AggregateId.");
    }

    kafkaTemplate.send(topicInfo.value(), aggregateId, message);
  }


}

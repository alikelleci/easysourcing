package com.github.easysourcing.messages;

import com.github.easysourcing.messages.annotations.TopicInfo;
import com.github.easysourcing.messages.exceptions.AggregateIdMissingException;
import com.github.easysourcing.messages.exceptions.PayloadMissingException;
import com.github.easysourcing.messages.exceptions.TopicInfoMissingException;
import org.springframework.kafka.core.KafkaTemplate;

public class MessageGateway {

  private final KafkaTemplate<String, Message> kafkaTemplate;

  protected MessageGateway(KafkaTemplate<String, Message> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void send(Message message) {
    if (message.getPayload() == null) {
      throw new PayloadMissingException("You are trying to dispatch a message without a payload.");
    }

    TopicInfo topicInfo = message.getTopicInfo();
    if (topicInfo == null) {
      throw new TopicInfoMissingException("You are trying to dispatch a message without any topic information. Please annotate your message with @TopicInfo.");
    }

    String aggregateId = message.getId();
    if (aggregateId == null) {
      throw new AggregateIdMissingException("You are trying to dispatch a message without a proper identifier. Please annotate your field containing the identifier with @AggregateId.");
    }

    kafkaTemplate.send(topicInfo.value(), aggregateId, message);
  }


}

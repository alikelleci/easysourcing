package com.github.easysourcing.messages;

import com.github.easysourcing.messages.annotations.TopicInfo;
import com.github.easysourcing.messages.exceptions.AggregateIdMissingException;
import com.github.easysourcing.messages.exceptions.PayloadMissingException;
import com.github.easysourcing.messages.exceptions.TopicInfoMissingException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class MessageGateway {

  private final Producer<String, Message> producer;

  public MessageGateway(Producer<String, Message> producer) {
    this.producer = producer;
  }

  public Future<RecordMetadata> send(Message message) {
    if (message.getPayload() == null) {
      throw new PayloadMissingException("You are trying to dispatch a message without a payload.");
    }

    TopicInfo topicInfo = message.getTopicInfo();
    if (topicInfo == null) {
      throw new TopicInfoMissingException("You are trying to dispatch a message without any topic information. Please annotate your message with @TopicInfo.");
    }

    String aggregateId = message.getAggregateId();
    if (aggregateId == null) {
      throw new AggregateIdMissingException("You are trying to dispatch a message without a proper identifier. Please annotate your field containing the identifier with @AggregateId.");
    }

    return producer.send(new ProducerRecord<>(topicInfo.value(), aggregateId, message));
  }


}

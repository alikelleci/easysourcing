package io.github.alikelleci.easysourcing.messages;

import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.easysourcing.common.exceptions.PayloadMissingException;
import io.github.alikelleci.easysourcing.common.exceptions.TopicInfoMissingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

@Slf4j
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

    log.debug("Sending {}: {} ({})", message.getMessageType(), message.getPayloadType(), message.getAggregateId());
    return producer.send(new ProducerRecord<>(topicInfo.value(), aggregateId, message));
  }


}

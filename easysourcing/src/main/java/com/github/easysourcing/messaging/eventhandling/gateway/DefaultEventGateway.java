package com.github.easysourcing.messaging.eventhandling.gateway;

import com.github.easysourcing.common.annotations.TopicInfo;
import com.github.easysourcing.common.exceptions.AggregateIdMissingException;
import com.github.easysourcing.common.exceptions.PayloadMissingException;
import com.github.easysourcing.common.exceptions.TopicInfoMissingException;
import com.github.easysourcing.messaging.Metadata;
import com.github.easysourcing.messaging.eventhandling.Event;
import com.github.easysourcing.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static com.github.easysourcing.messaging.Metadata.CORRELATION_ID;
import static com.github.easysourcing.messaging.Metadata.ID;

@Slf4j
public class DefaultEventGateway implements EventGateway {

  private final Producer<String, Event> producer;

  protected DefaultEventGateway(Properties producerConfig) {
    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());
  }

  @Override
  public void publish(Object payload, Metadata metadata, Instant timestamp) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    Event event = Event.builder()
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(ID, UUID.randomUUID().toString())
            .entry(CORRELATION_ID, UUID.randomUUID().toString())
            .build())
        .build();

    validate(event);
    ProducerRecord<String, Event> record = new ProducerRecord<>(event.getTopicInfo().value(), null, timestamp.toEpochMilli(), event.getAggregateId(), event);

    log.debug("Publishing event: {} ({})", event.getType(), event.getAggregateId());
    producer.send(record);
  }

  private void validate(Event message) {
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
  }
}

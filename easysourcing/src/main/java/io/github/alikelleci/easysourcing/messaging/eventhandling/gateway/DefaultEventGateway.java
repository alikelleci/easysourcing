package io.github.alikelleci.easysourcing.messaging.eventhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.easysourcing.common.exceptions.PayloadMissingException;
import io.github.alikelleci.easysourcing.common.exceptions.TopicInfoMissingException;
import io.github.alikelleci.easysourcing.messaging.Metadata;
import io.github.alikelleci.easysourcing.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static io.github.alikelleci.easysourcing.messaging.Metadata.CORRELATION_ID;

@Slf4j
public class DefaultEventGateway implements EventGateway {

  private final Producer<String, Event> producer;

  protected DefaultEventGateway(Properties producerConfig, ObjectMapper objectMapper) {
    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(Event.class, objectMapper));
  }

  @Override
  public void publish(Object payload, Metadata metadata, Instant timestamp) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    if (timestamp == null) {
      timestamp = Instant.now();
    }

    Event event = Event.builder()
        .payload(payload)
        .metadata(Metadata.builder()
            .addAll(metadata)
            .add(CORRELATION_ID, UUID.randomUUID().toString())
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

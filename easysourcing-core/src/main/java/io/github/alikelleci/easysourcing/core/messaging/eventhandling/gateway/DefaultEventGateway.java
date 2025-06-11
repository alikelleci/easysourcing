package io.github.alikelleci.easysourcing.core.messaging.eventhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.core.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.core.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.easysourcing.core.common.exceptions.PayloadMissingException;
import io.github.alikelleci.easysourcing.core.common.exceptions.TopicInfoMissingException;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.core.support.serialization.json.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static io.github.alikelleci.easysourcing.core.messaging.Metadata.CORRELATION_ID;

@Slf4j
public class DefaultEventGateway implements EventGateway {

  private final Producer<String, Event> producer;

  protected DefaultEventGateway(Properties producerConfig, ObjectMapper objectMapper) {
    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(objectMapper));
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
    ProducerRecord<String, Event> producerRecord = new ProducerRecord<>(event.getTopicInfo().value(), null, timestamp.toEpochMilli(), event.getAggregateId(), event);

    log.debug("Publishing event: {} ({})", event.getType(), event.getAggregateId());
    producer.send(producerRecord);
  }

  private void validate(Event event) {
    if (event.getPayload() == null) {
      throw new PayloadMissingException("You are trying to publish an event without a payload.");
    }

    TopicInfo topicInfo = event.getTopicInfo();
    if (topicInfo == null) {
      throw new TopicInfoMissingException("You are trying to publish an event without any topic information. Please annotate your event with @TopicInfo.");
    }

    String aggregateId = event.getAggregateId();
    if (aggregateId == null) {
      throw new AggregateIdMissingException("You are trying to publish an event without a proper identifier. Please annotate your field containing the identifier with @AggregateId.");
    }
  }
}

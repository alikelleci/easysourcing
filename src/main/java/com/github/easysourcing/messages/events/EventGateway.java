package com.github.easysourcing.messages.events;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.UUID;
import java.util.concurrent.Future;

import static com.github.easysourcing.messages.MetadataKeys.CORRELATION_ID;
import static com.github.easysourcing.messages.MetadataKeys.ID;

@Slf4j
public class EventGateway extends MessageGateway {

  public EventGateway(Producer<String, Message> kafkaProducer) {
    super(kafkaProducer);
  }

  public Future<RecordMetadata> publish(Object payload, Metadata metadata) {
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

    if (log.isDebugEnabled()) {
      log.debug("Publishing event: {}", event);
    } else if (log.isInfoEnabled()) {
      log.info("Publishing event: {} ({})", event.getType(), event.getAggregateId());
    }
    return send(event);
  }

  public Future<RecordMetadata> publish(Object payload) {
    return this.publish(payload, null);
  }

}

package io.github.alikelleci.easysourcing.core.events;

import io.github.alikelleci.easysourcing.common.Message;
import io.github.alikelleci.easysourcing.common.MessageGateway;
import io.github.alikelleci.easysourcing.common.Metadata;
import io.github.alikelleci.easysourcing.common.MetadataKeys;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.UUID;
import java.util.concurrent.Future;

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
            .entry(MetadataKeys.ID, UUID.randomUUID().toString())
            .entry(MetadataKeys.CORRELATION_ID, UUID.randomUUID().toString())
            .build())
        .build();

    log.debug("Publishing event: {} ({})", event.getType(), event.getAggregateId());
    return send(event);
  }

  public Future<RecordMetadata> publish(Object payload) {
    return this.publish(payload, null);
  }

}

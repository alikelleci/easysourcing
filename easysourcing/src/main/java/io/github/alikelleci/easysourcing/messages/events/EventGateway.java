package io.github.alikelleci.easysourcing.messages.events;

import io.github.alikelleci.easysourcing.messages.Message;
import io.github.alikelleci.easysourcing.messages.MessageGateway;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.MetadataKeys;
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
      metadata = new Metadata();
    }
    Event event = Event.builder()
        .payload(payload)
        .metadata(metadata.filter()
            .add(MetadataKeys.ID, UUID.randomUUID().toString())
            .add(MetadataKeys.CORRELATION_ID, UUID.randomUUID().toString()))
        .build();

    return send(event);
  }

  public Future<RecordMetadata> publish(Object payload) {
    return this.publish(payload, null);
  }

}

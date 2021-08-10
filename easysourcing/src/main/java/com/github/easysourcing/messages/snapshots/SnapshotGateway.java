package com.github.easysourcing.messages.snapshots;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.Metadata;
import com.github.easysourcing.messages.MetadataKeys;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.UUID;
import java.util.concurrent.Future;

@Slf4j
public class SnapshotGateway extends MessageGateway {

  public SnapshotGateway(Producer<String, Message> kafkaProducer) {
    super(kafkaProducer);
  }

  public Future<RecordMetadata> publish(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    Aggregate snapshot = Aggregate.builder()
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(MetadataKeys.ID, UUID.randomUUID().toString())
            .entry(MetadataKeys.CORRELATION_ID, UUID.randomUUID().toString())
            .build())
        .build();

    log.debug("Publishing snapshot: {} ({})", snapshot.getType(), snapshot.getAggregateId());
    return send(snapshot);
  }

  public Future<RecordMetadata> publish(Object payload) {
    return this.publish(payload, null);
  }

}

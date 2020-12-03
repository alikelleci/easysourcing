package com.github.easysourcing.messages.snapshots;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.Metadata;
import com.github.easysourcing.messages.aggregates.Aggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;

import java.util.UUID;

import static com.github.easysourcing.messages.MetadataKeys.CORRELATION_ID;
import static com.github.easysourcing.messages.MetadataKeys.ID;

@Slf4j
public class SnapshotGateway extends MessageGateway {

  public SnapshotGateway(Producer<String, Message> kafkaProducer) {
    super(kafkaProducer);
  }

  public void publish(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    Aggregate snapshot = Aggregate.builder()
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(ID, UUID.randomUUID().toString())
            .entry(CORRELATION_ID, UUID.randomUUID().toString())
            .build())
        .build();

    if (log.isDebugEnabled()) {
      log.debug("Publishing snapshot: {}", snapshot);
    } else if (log.isInfoEnabled()) {
      log.info("Publishing snapshot: {} ({})", snapshot.getType(), snapshot.getAggregateId());
    }
    send(snapshot);
  }

  public void publish(Object payload) {
    this.publish(payload, null);
  }

}

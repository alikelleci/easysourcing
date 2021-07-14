package io.github.alikelleci.easysourcing.messages.snapshots;

import io.github.alikelleci.easysourcing.Gateway;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

@Slf4j
public class SnapshotGateway extends Gateway {

  public SnapshotGateway(Producer<String, Object> producer) {
    super(producer);
  }

  @Override
  public Future<RecordMetadata> send(Object payload, Metadata metadata) {
    log.debug("Publishing snapshot: {} ({})", payload.getClass().getSimpleName(), CommonUtils.getAggregateId(payload));
    return super.send(payload, metadata);
  }

}

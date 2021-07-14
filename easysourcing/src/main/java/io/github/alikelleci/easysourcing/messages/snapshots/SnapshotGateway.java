package io.github.alikelleci.easysourcing.messages.snapshots;

import io.github.alikelleci.easysourcing.Gateway;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;

@Slf4j
public class SnapshotGateway extends Gateway {

  public SnapshotGateway(Producer<String, Object> producer) {
    super(producer);
  }

  @Override
  public void send(Object payload, Metadata metadata) {
    log.debug("Publishing snapshot: {} ({})", payload.getClass().getSimpleName(), CommonUtils.getAggregateId(payload));
    super.send(payload, metadata);
  }

}

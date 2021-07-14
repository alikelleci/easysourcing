package io.github.alikelleci.easysourcing.messages.snapshots;

import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Future;

@Slf4j
public class SnapshotGateway {

  private final Producer<String, Object> producer;

  public SnapshotGateway(Producer<String, Object> producer) {
    this.producer = producer;
  }

  protected Future<RecordMetadata> send(ProducerRecord<String, Object> record) {
    String id = UUID.randomUUID().toString();
    String correlationId = UUID.randomUUID().toString();

    record.headers()
        .remove(Metadata.ID)
        .add(Metadata.ID, id.getBytes(StandardCharsets.UTF_8))
        .remove(Metadata.CORRELATION_ID)
        .add(Metadata.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));

    log.debug("Publishing snapshot: {} ({})", record.value().getClass().getSimpleName(), CommonUtils.getAggregateId(record.value()));
    return producer.send(record);
  }

  public Future<RecordMetadata> publish(Object payload, Metadata metadata) {
    CommonUtils.validatePayload(payload);

    ProducerRecord<String, Object> record = new ProducerRecord<>(CommonUtils.getTopicInfo(payload).value(), CommonUtils.getAggregateId(payload), payload);

    if (metadata == null) {
      metadata = Metadata.builder().build();
    }
    metadata.filter().getEntries().entrySet().stream()
        .filter(entry -> StringUtils.isNoneBlank(entry.getKey(), entry.getValue()))
        .forEach(entry ->
            record.headers()
                .remove(entry.getKey())
                .add(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8)));

    return producer.send(record);
  }

  public Future<RecordMetadata> publish(Object payload) {
    return this.publish(payload, null);
  }

}

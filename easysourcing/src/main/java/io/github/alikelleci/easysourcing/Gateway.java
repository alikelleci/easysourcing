package io.github.alikelleci.easysourcing;

import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
public abstract class Gateway {

  protected final Producer<String, Object> producer;

  public Gateway(Producer<String, Object> producer) {
    this.producer = producer;
  }

  protected ProducerRecord<String, Object> createProducerRecord(Object payload, Metadata metadata) {
    CommonUtils.validatePayload(payload);

    ProducerRecord<String, Object> record = new ProducerRecord<>(CommonUtils.getTopicInfo(payload).value(), CommonUtils.getAggregateId(payload), payload);

    String id = UUID.randomUUID().toString();
    String correlationId = UUID.randomUUID().toString();

    record.headers()
        .remove(Metadata.ID)
        .add(Metadata.ID, id.getBytes(StandardCharsets.UTF_8))
        .remove(Metadata.CORRELATION_ID)
        .add(Metadata.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));

    if (metadata == null) {
      metadata = Metadata.builder().build();
    }
    metadata.filter().getEntries().entrySet().stream()
        .filter(entry -> StringUtils.isNoneBlank(entry.getKey(), entry.getValue()))
        .forEach(entry ->
            record.headers()
                .remove(entry.getKey())
                .add(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8)));

    return record;
  }


  public void send(Object payload, Metadata metadata) {
    ProducerRecord<String, Object> record = createProducerRecord(payload, metadata);
    producer.send(record);
  }


  public void send(Object payload) {
    this.send(payload, null);
  }

}

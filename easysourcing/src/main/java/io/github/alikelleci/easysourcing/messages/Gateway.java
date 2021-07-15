package io.github.alikelleci.easysourcing.messages;

import io.github.alikelleci.easysourcing.util.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public interface Gateway {

  default ProducerRecord<String, Object> createProducerRecord(Object payload, Metadata metadata) {
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

}
